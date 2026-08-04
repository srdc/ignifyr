package io.ignifyr.engine.config

import com.typesafe.config.Config
import io.ignifyr.engine.spi.ExtensionRegistry
import io.ignifyr.engine.util.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._
import scala.util.Try

object IgnifyrConfig {

  import io.ignifyr.engine.Execution.actorSystem
  protected lazy val config: Config = actorSystem.settings.config

  /**
   * Ignifyr Engine configurations
   */
  lazy val engineConfig = new IgnifyrEngineConfig(config.getConfig("ignifyr"))

  /**
   * Spark configurations
   */
  private lazy val sparkConfig: Config = config.getConfig("spark")

  /** Application name for Spark */
  private lazy val sparkAppName: String =
    Try(sparkConfig.getString("app.name")).getOrElse("Ignifyr")

  /** Master url of the Spark cluster */
  private lazy val sparkMaster: String = Try(sparkConfig.getString("master")).getOrElse("local[4]")

  /** Directory to keep Spark's checkpoints created  */
  lazy val sparkCheckpointDirectory: String =
    FileUtils.getPath(Try(sparkConfig.getString("checkpoint-dir")).getOrElse("checkpoint")).toString

  /**
   * Default configurations for spark
   */
  private val sparkConfDefaults: Map[String, String] =
    Map(
      "spark.driver.allowMultipleContexts" -> "false",
      "spark.sql.caseSensitive" -> "true", // Enable case sensitivity to treat schema column names as case-sensitive to avoid potential conflicts
      "spark.sql.files.ignoreCorruptFiles" -> "false", // Do not ignore corrupted files (e.g. CSV missing a field from the given schema) as we want to log them
      "spark.sql.streaming.checkpointLocation" -> sparkCheckpointDirectory, // Checkpoint directory for streaming
      "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs" -> "false" // Do not create _SUCCESS file while writing to csv
      // Spark session extensions/catalogs required by specific formats (e.g. the Delta Lake session
      // extension + catalog) are contributed by their modules via IgnifyrExtension.sparkConfContributions,
      // so the community engine carries no such wiring. See ExtensionRegistry.sparkConfContributions.
    )
  // Spark session
  lazy val sparkSession: SparkSession = SparkSession.builder().config(createSparkConf).getOrCreate()

  /**
   * Spark-conf keys that Spark parses as a **comma-separated list of things to register or ship**.
   * For these the layers below must be *joined*: dropping one layer's entries is always a bug, since
   * each entry is an independent registration rather than a value someone means to replace.
   *
   * Deliberately limited to that shape. A key like `spark.driver.extraClassPath` looks list-like but
   * is separated by the platform path separator, not commas, and a scalar such as
   * `spark.sql.catalog.spark_catalog` (Delta's other contribution) is genuinely single-valued — a user
   * override there is legitimate and must keep winning. Add a key here only when Spark treats it as a
   * comma-separated registration list.
   */
  private val additiveSparkConfKeys: Set[String] = Set(
    "spark.sql.extensions",
    "spark.plugins",
    "spark.jars",
    "spark.jars.packages",
    "spark.jars.repositories",
    "spark.extraListeners",
    "spark.sql.queryExecutionListeners",
    "spark.sql.streaming.streamingQueryListeners"
  )

  /**
   * Create spark configuration from this config.
   *
   * Three layers, lowest precedence first: engine defaults, module contributions
   * ([[ExtensionRegistry.sparkConfContributions]]), then the user's `spark { }` block.
   */
  private def createSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
      .setAppName(sparkAppName)
      .setMaster(sparkMaster)

    val contributed: Map[String, String] = ExtensionRegistry.sparkConfContributions
    val userProvided: Map[String, String] = sparkConfig
      .entrySet()
      .asScala
      .filter(e => e.getKey != "app.name" && e.getKey != "master")
      .map(e => s"spark.${e.getKey}" -> e.getValue.unwrapped().toString)
      .toMap

    // `++` replaces on collision, which is what a single-valued key wants: the user's `spark { }` block
    // wins over a module contribution, which wins over the engine default.
    val merged = sparkConfDefaults ++ contributed ++ userProvided

    // A list-valued key must be joined across layers instead. `ExtensionRegistry` already concatenates
    // `spark.sql.extensions` across modules; without this the user layer would then overwrite that merged
    // value, so anyone setting `spark.sql.extensions` for an unrelated reason (Iceberg, Sedona, a custom
    // optimizer rule) would silently drop the Delta session extension contributed by
    // `ignifyr-format-delta` — and the resulting Delta write failure points nowhere near the config.
    val joined = additiveSparkConfKeys.flatMap { key =>
      val values = Seq(sparkConfDefaults.get(key), contributed.get(key), userProvided.get(key)).flatten
        .flatMap(_.split(","))
        .map(_.trim)
        .filter(_.nonEmpty)
        .distinct
      if (values.isEmpty) None else Some(key -> values.mkString(","))
    }.toMap

    (merged ++ joined)
      .foldLeft(sparkConf) { case (sc, e) =>
        sc.set(e._1, e._2)
      }
  }
}
