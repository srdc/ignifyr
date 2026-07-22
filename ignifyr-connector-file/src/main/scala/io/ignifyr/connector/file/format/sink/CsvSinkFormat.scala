package io.ignifyr.connector.file.format.sink

import io.ignifyr.connector.file.format.{FileSinkFormat, FileSinkSupport}
import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings, SinkContentTypes}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Community CSV file sink format. CSV is a flat structure, so only primitive (non-array, non-struct)
 * columns of the mapped resources are written. Partition-by-resource-type is not applicable to CSV.
 */
class CsvSinkFormat extends FileSinkFormat {

  override val contentTypes: Seq[String] = Seq(SinkContentTypes.CSV)

  override def write(
      spark: SparkSession,
      df: Dataset[FhirMappingResult],
      sinkSettings: FileSystemSinkSettings
  ): Unit = {
    import spark.implicits._
    // read the mapped resource json column and load it to a new data frame
    val mappedResourceDF = spark.read.json(df.select("mappedFhirResource.mappedResource").as[String])
    // select the columns that are not array type or struct type since the CSV is a flat data structure
    val nonArrayAndStructCols = mappedResourceDF.schema.fields
      .filterNot(field => field.dataType.isInstanceOf[ArrayType] || field.dataType.isInstanceOf[StructType])
      .map(_.name)
    // if the DataFrame contains data, write it to the specified path
    if (!mappedResourceDF.isEmpty) {
      val filteredDF = mappedResourceDF.select(nonArrayAndStructCols.head, nonArrayAndStructCols.tail: _*)
      FileSinkSupport.getWriter(filteredDF, sinkSettings).csv(sinkSettings.path)
    }
  }
}
