package io.ignifyr.engine.data.write

import io.ignifyr.engine.model.{FhirMappingResult, FhirSinkSettings}
import io.ignifyr.engine.spi.{ExtensionHints, ExtensionRegistry, MissingSinkException}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator

/**
 * Base class for FHIR resource writer
 *
 * @param sinkSettings
 */
abstract class BaseFhirWriter(sinkSettings: FhirSinkSettings) extends Serializable {

  /**
   * Write the data frame of json serialized FHIR resources to given sink (e.g. FHIR repository)
   *
   * @param df
   */
  def write(
      sparkSession: SparkSession,
      df: Dataset[FhirMappingResult],
      problemsAccumulator: CollectionAccumulator[FhirMappingResult]
  ): Unit

  /**
   * Validates the current FHIR writer. This method should be implemented by concrete subclasses to perform any necessary validation checks.
   * If the validation fails, an exception should be thrown.
   */
  def validate(): Unit
}

/**
 * Resolves a [[BaseFhirWriter]] for given sink settings from the extension registry. Kept as a thin
 * facade over `ExtensionRegistry.sinkProviders` so existing call sites are unchanged; a sink type
 * with no registered provider fails with an actionable [[MissingSinkException]].
 */
object FhirWriterFactory {
  def apply(sinkSettings: FhirSinkSettings): BaseFhirWriter =
    ExtensionRegistry.sinkProviders
      .getOrElse(sinkSettings.getClass, throw MissingSinkException(ExtensionHints.describeSink(sinkSettings.getClass)))
      .createWriter(sinkSettings)
}
