package io.ignifyr.engine.data.write

import io.ignifyr.engine.model.{FhirMappingResult, SinkSettings}
import io.ignifyr.engine.spi.{ExtensionHints, ExtensionRegistry, MissingSinkException}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator

/**
 * Base class for a sink writer: writes mapped results to one sink type (e.g. a FHIR repository,
 * the file system, a future relational target).
 *
 * @param sinkSettings
 */
abstract class BaseSinkWriter(sinkSettings: SinkSettings) extends Serializable {

  /**
   * Write the data frame of JSON-serialized mapped resources to the sink (e.g. FHIR repository)
   *
   * @param df
   */
  def write(
      sparkSession: SparkSession,
      df: Dataset[FhirMappingResult],
      problemsAccumulator: CollectionAccumulator[FhirMappingResult]
  ): Unit

  /**
   * Validates the current sink writer. This method should be implemented by concrete subclasses to perform any necessary validation checks.
   * If the validation fails, an exception should be thrown.
   */
  def validate(): Unit
}

/**
 * Resolves a [[BaseSinkWriter]] for given sink settings from the extension registry. Kept as a thin
 * facade over `ExtensionRegistry.sinkProviders` so existing call sites are unchanged; a sink type
 * with no registered provider fails with an actionable [[MissingSinkException]].
 */
object SinkWriterFactory {
  def apply(sinkSettings: SinkSettings): BaseSinkWriter =
    ExtensionRegistry.sinkProviders
      .getOrElse(sinkSettings.getClass, throw MissingSinkException(ExtensionHints.describeSink(sinkSettings.getClass)))
      .createWriter(sinkSettings)
}
