package io.ignifyr.sink.file

import io.ignifyr.sink.file.format.FileSinkFormatRegistry
import io.ignifyr.engine.data.write.BaseSinkWriter
import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator

/**
 * Writes mapped FHIR resources to the file system. A thin dispatcher: it resolves the
 * [[io.ignifyr.sink.file.format.FileSinkFormat]] for the sink's content type from the
 * [[FileSinkFormatRegistry]] and delegates the write, so the set of supported output formats is
 * pluggable (e.g. Delta via the enterprise `ignifyr-format-delta`). A content type with no installed
 * handler fails with an actionable `MissingFileSinkFormatException`.
 */
class FileSystemWriter(sinkSettings: FileSystemSinkSettings) extends BaseSinkWriter(sinkSettings) {

  override def write(
      spark: SparkSession,
      df: Dataset[FhirMappingResult],
      problemsAccumulator: CollectionAccumulator[FhirMappingResult]
  ): Unit =
    FileSinkFormatRegistry.sinkFormat(sinkSettings.contentType).write(spark, df, sinkSettings)

  /**
   * Validates the current sink writer. For the FileSystemWriter, validation is not implemented and
   * this method does nothing.
   */
  override def validate(): Unit = {}
}
