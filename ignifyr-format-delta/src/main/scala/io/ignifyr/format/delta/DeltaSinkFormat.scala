package io.ignifyr.format.delta

import io.ignifyr.connector.file.format.{FileSinkFormat, FileSinkSupport}
import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings, SinkContentTypes}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Enterprise Delta Lake file *sink* format (optionally partitioned by FHIR resource type),
 * contributed to the file connector's format sub-SPI via ServiceLoader. The Spark-session wiring
 * Delta needs (`spark.sql.extensions` + the Delta catalog) is contributed by
 * [[DeltaFormatExtension]] through IgnifyrExtension.sparkConfContributions, and the delta-spark
 * dependency lives in this module — keeping Delta out of the community engine and jar.
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
