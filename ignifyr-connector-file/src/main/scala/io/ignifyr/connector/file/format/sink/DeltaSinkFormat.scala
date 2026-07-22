package io.ignifyr.connector.file.format.sink

import io.ignifyr.connector.file.format.{FileSinkFormat, FileSinkSupport}
import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings, SinkContentTypes}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Delta Lake file sink format. Optionally partitions the output by FHIR resource type.
 *
 * NOTE: this handler (and the delta-spark dependency + the Delta Spark-session wiring it needs) is
 * extracted into the enterprise `ignifyr-format-delta` module in a later step; it lives here only
 * while the file connector is first carved out of the engine. The write is string-based
 * (`.format("delta")`), so it compiles without delta-spark but requires it at runtime.
 */
class DeltaSinkFormat extends FileSinkFormat {

  override val contentTypes: Seq[String] = Seq(SinkContentTypes.DELTA_LAKE)

  override def write(
      spark: SparkSession,
      df: Dataset[FhirMappingResult],
      sinkSettings: FileSystemSinkSettings
  ): Unit = {
    import spark.implicits._
    if (sinkSettings.partitionByResourceType) {
      FileSinkSupport.writePartitionedByResourceType(
        spark,
        df,
        sinkSettings,
        singleColumnJson = false,
        (writer, outputPath) => writer.format(SinkContentTypes.DELTA_LAKE).save(outputPath)
      )
    } else {
      // Convert the mapped resource JSON strings into a frame and write it as Delta Lake.
      val jsonDS = df.select("mappedFhirResource.mappedResource").as[String]
      val jsonDF = spark.read.json(jsonDS)
      FileSinkSupport.getWriter(jsonDF, sinkSettings).format(SinkContentTypes.DELTA_LAKE).save(sinkSettings.path)
    }
  }
}
