package io.ignifyr.engine.spi

import io.ignifyr.engine.data.read.BaseDataSourceReader
import io.ignifyr.engine.model.{MappingJobSourceSettings, MappingSourceBinding}
import org.apache.spark.sql.SparkSession

/**
 * Contributes a data-source reader for one source-binding type.
 *
 * The binding/settings model classes always live in the engine, so a job JSON referencing this
 * source parses everywhere; when no connector is registered for the binding class, the engine
 * raises a [[MissingConnectorException]] at read time rather than failing to parse.
 */
trait SourceConnector {

  /** Stable identifier, e.g. "file", "sql", "kafka". */
  def id: String

  /** Binding model class this connector reads (the registry lookup key). */
  def bindingClass: Class[_ <: MappingSourceBinding]

  /** Source-settings model class this connector expects. */
  def settingsClass: Class[_ <: MappingJobSourceSettings]

  /**
   * Construct the reader for a given Spark session. The concrete reader is parameterized on the
   * binding/settings types; callers cast to their statically-known types, mirroring the previous
   * `DataSourceReaderFactory` behaviour.
   */
  def createReader(spark: SparkSession): BaseDataSourceReader[_, _]
}
