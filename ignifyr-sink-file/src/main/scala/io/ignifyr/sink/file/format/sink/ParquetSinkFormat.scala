package io.ignifyr.sink.file.format.sink

import io.ignifyr.sink.file.format.{FileSinkFormat, FileSinkSupport}
import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings, SinkContentTypes}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Community Parquet file sink format. Optionally partitions the output by FHIR resource type (and by
 * the configured partitioning columns).
 */
class ParquetSinkFormat extends FileSinkFormat {

  override val contentTypes: Seq[String] = Seq(SinkContentTypes.PARQUET)

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
        (writer, outputPath) => writer.parquet(outputPath)
      )
    } else {
      // Convert the mapped resource JSON strings into a frame and write it as Parquet.
      val jsonDS = df.select("mappedFhirResource.mappedResource").as[String]
      val jsonDF = spark.read.json(jsonDS)
      FileSinkSupport.getWriter(jsonDF, sinkSettings).parquet(sinkSettings.path)
    }
  }
}
