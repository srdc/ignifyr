package io.ignifyr.connector.file

import io.ignifyr.connector.file.format.FileFormatRegistry
import io.ignifyr.engine.data.write.BaseFhirWriter
import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator

/**
 * Writes mapped FHIR resources to the file system. A thin dispatcher: it resolves the
 * [[io.ignifyr.connector.file.format.FileSinkFormat]] for the sink's content type from the
 * [[FileFormatRegistry]] and delegates the write, so the set of supported output formats is
 * pluggable (e.g. Delta via the enterprise `ignifyr-format-delta`). A content type with no installed
 * handler fails with an actionable `MissingFileFormatException`.
 */
class FileSystemWriter(sinkSettings: FileSystemSinkSettings) extends BaseFhirWriter(sinkSettings) {

  override def write(
      spark: SparkSession,
      df: Dataset[FhirMappingResult],
      problemsAccumulator: CollectionAccumulator[FhirMappingResult]
  ): Unit =
    FileFormatRegistry.sinkFormat(sinkSettings.contentType).write(spark, df, sinkSettings)

  /**
   * Validates the current FHIR writer. For the FileSystemWriter, validation is not implemented and
   * this method does nothing.
   */
  override def validate(): Unit = {}
}
