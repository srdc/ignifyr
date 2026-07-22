package io.ignifyr.format.delta

import io.ignifyr.engine.spi.IgnifyrExtension

/**
 * [[IgnifyrExtension]] for the enterprise Delta Lake sink format. It contributes nothing to the
 * engine's connector/sink registries directly — the Delta writer plugs into the file connector's
 * format sub-SPI ([[DeltaSinkFormat]], via its own META-INF/services entry). Its role here is to
 * contribute the Spark-session wiring Delta needs, so the community engine carries no Delta config:
 * when this module is installed, the shared SparkSession is built with the Delta session extension
 * and the Delta catalog.
 */
class DeltaFormatExtension extends IgnifyrExtension {

  override val id: String = "format-delta"

  override def sparkConfContributions: Map[String, String] = Map(
    // Enable Delta Lake features by adding the DeltaSparkSessionExtension to the Spark session.
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    // Use DeltaCatalog as the default catalog for managing Delta tables in Spark.
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  )
}
