package io.ignifyr.engine.spi

import io.ignifyr.engine.model.{MappingJobSourceSettings, MappingSourceBinding}
import org.apache.spark.sql.types.StructType

/**
 * Lets a connector infer the flat schema of a source binding through connector-native means,
 * instead of the engine's generic fallback (a one-row Spark read of the source).
 *
 * The prime example is SQL: reading JDBC `ResultSetMetaData` yields the table/query schema without
 * pulling any data. A connector contributes an inferrer keyed by its settings class; callers (e.g.
 * the server's schema-inference endpoint) look it up in [[ExtensionRegistry.schemaInferrers]] and
 * fall back to the generic Spark read when no inferrer is registered — or when the inferrer itself
 * returns None for a binding it prefers Spark to handle.
 */
trait SourceSchemaInferrer {

  /** Short identifier of the providing connector, e.g. "sql" (used in error reporting). */
  def id: String

  /** The source-settings class this inferrer handles; the registry key. */
  def settingsClass: Class[_ <: MappingJobSourceSettings]

  /**
   * Infer the schema of the given source binding.
   *
   * @return the inferred schema, or None to fall back to the generic Spark-read inference.
   */
  def inferSchema(sourceBinding: MappingSourceBinding, sourceSettings: MappingJobSourceSettings): Option[StructType]
}
