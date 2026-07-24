package io.ignifyr.sink.file.format.sink

import io.ignifyr.sink.file.format.{FileSinkFormat, FileSinkSupport}
import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings, SinkContentTypes}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Community NDJSON file sink format (the FHIR bulk-data output). Optionally partitions the output by
 * FHIR resource type.
 */
class NdjsonSinkFormat extends FileSinkFormat {

  override val contentTypes: Seq[String] = Seq(SinkContentTypes.NDJSON)

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
        singleColumnJson = true,
        (writer, outputPath) => writer.text(outputPath)
      )
    } else {
      FileSinkSupport
        .getWriter(df.map(_.mappedFhirResource.get.mappedResource.get), sinkSettings)
        .text(sinkSettings.path)
    }
  }
}
