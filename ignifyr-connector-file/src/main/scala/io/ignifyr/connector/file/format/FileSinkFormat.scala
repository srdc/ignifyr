package io.ignifyr.connector.file.format

import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * A file *sink* format handler contributed to the file connector: writes a dataset of mapped FHIR
 * resources to the file system in one content type (e.g. `ndjson`, `csv`, `parquet`, `delta`).
 *
 * Like [[FileSourceFormat]] this is a sub-SPI owned by `ignifyr-connector-file` and discovered via
 * its own [[java.util.ServiceLoader]] (a `META-INF/services/io.ignifyr.connector.file.format.FileSinkFormat`
 * entry per module). The community connector ships the ndjson/csv/parquet writers (FHIR bulk output);
 * the enterprise `ignifyr-format-delta` contributes the Delta writer and carries the delta-spark
 * dependency, so it stays out of the community jar. Shared write machinery (the partition-by-resource-type
 * layout, the configured DataFrameWriter) lives in [[FileSinkSupport]] so a format module can reuse it.
 */
trait FileSinkFormat {

  /** Content types (as they appear in `FileSystemSinkSettings.contentType`) this format writes. */
  def contentTypes: Seq[String]

  /** Writes the mapped FHIR resources to `sinkSettings.path` in this format. */
  def write(spark: SparkSession, df: Dataset[FhirMappingResult], sinkSettings: FileSystemSinkSettings): Unit
}
