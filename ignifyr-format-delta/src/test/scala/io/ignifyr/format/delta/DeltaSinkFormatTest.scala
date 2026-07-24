package io.ignifyr.format.delta

import io.ignifyr.FhirMappingResultFixtures
import io.ignifyr.sink.file.FileSystemWriter
import io.ignifyr.sink.file.format.FileSinkFormatRegistry
import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings, SinkContentTypes}
import io.ignifyr.engine.util.FileUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests the enterprise Delta Lake sink format: it is discovered through the file connector's format
 * registry when this module is on the classpath, and the file writer writes Delta output (plain and
 * partitioned by resource type) through it. These are the Delta cases relocated from the connector's
 * FileSystemWriterTest; they rely on the Delta Spark-session wiring this module contributes via
 * DeltaFormatExtension.sparkConfContributions (so the shared SparkSession is built with the Delta
 * session extension + catalog). The shared test dataset comes from the testkit.
 */
class DeltaSinkFormatTest extends AnyFlatSpec with BeforeAndAfterAll with Matchers {

  val sparkSession: SparkSession = IgnifyrConfig.sparkSession
  import sparkSession.implicits._

  val df: Dataset[FhirMappingResult] = FhirMappingResultFixtures.sampleFhirMappingResults(sparkSession)

  "The Delta format" should "be discovered for the delta content type" in {
    FileSinkFormatRegistry.sinkFormats.keySet should contain(SinkContentTypes.DELTA_LAKE)
  }

  it should "write DataFrame into a Delta Lake file" in {
    val outputFolderPath = s"${IgnifyrConfig.engineConfig.contextPath}/output-delta"
    val fileSystemWriter = new FileSystemWriter(sinkSettings =
      FileSystemSinkSettings(path = outputFolderPath, contentType = SinkContentTypes.DELTA_LAKE)
    )
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    val writtenDf = sparkSession.read
      .format(SinkContentTypes.DELTA_LAKE)
      .load(outputFolderPath)
    writtenDf.count() shouldBe 15
    val resultDf = writtenDf.groupBy("resourceType").count()
    val patientCount = resultDf.filter(col("resourceType") === "Patient").select("count").as[Long].head()
    patientCount shouldBe 10
    val conditionCount = resultDf.filter(col("resourceType") === "Condition").select("count").as[Long].head()
    conditionCount shouldBe 5
  }

  it should "write DataFrame as partitioned Delta Lake files based on resource type" in {
    val outputFolderPath = s"${IgnifyrConfig.engineConfig.contextPath}/output-delta-by-resource"
    val fileSystemWriter = new FileSystemWriter(sinkSettings =
      FileSystemSinkSettings(
        path = outputFolderPath,
        contentType = SinkContentTypes.DELTA_LAKE,
        partitionByResourceType = true
      )
    )
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    val conditionDf = sparkSession.read
      .format(SinkContentTypes.DELTA_LAKE)
      .load(s"$outputFolderPath/Condition")
    conditionDf.count() shouldBe 5
    val patientDf = sparkSession.read
      .format(SinkContentTypes.DELTA_LAKE)
      .load(s"$outputFolderPath/Patient")
    patientDf.count() shouldBe 10
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    // delete the context path used for the output folders
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath("").toFile)
  }
}
