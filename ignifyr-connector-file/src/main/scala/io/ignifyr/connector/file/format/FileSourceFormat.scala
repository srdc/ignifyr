package io.ignifyr.connector.file.format

import io.ignifyr.engine.model.{FileSystemSource, FileSystemSourceSettings}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A file *source* format handler contributed to the file connector: given a resolved path it turns
 * one file content type (e.g. `csv`, `json`, `parquet`) into a Spark [[DataFrame]].
 *
 * This is a sub-SPI owned by `ignifyr-connector-file` and discovered through its own
 * [[java.util.ServiceLoader]] (a `META-INF/services/io.ignifyr.connector.file.format.FileSourceFormat`
 * entry per module), so additional formats — e.g. the enterprise `ignifyr-format-json` — plug in
 * without the engine or the file connector knowing about them. The engine stays entirely ignorant
 * of file formats; only the file connector dispatches on content type.
 *
 * Cross-cutting concerns (path resolution, the streaming directory check, the streaming
 * filename-logging column, and the `distinct` option) are handled once by
 * [[io.ignifyr.connector.file.FileDataSourceReader]]; a format only produces the raw frame.
 */
trait FileSourceFormat {

  /** Content types (as they appear in `FileSystemSource.contentType`) this format reads. */
  def contentTypes: Seq[String]

  /** Reads the raw frame for the given context (no filename column, no `distinct` applied). */
  def read(ctx: FileSourceReadContext): DataFrame
}

/**
 * Everything a [[FileSourceFormat]] needs, prepared by the reader.
 *
 * @param spark                    the Spark session
 * @param finalPath                the fully-resolved file/directory path to read
 * @param isZipFile                whether `mappingSourceBinding.path` points to a `.zip` archive
 * @param mappingSourceBinding     the source binding (content type, options, preprocessSql, ...)
 * @param mappingJobSourceSettings the job-level source settings (data folder, `asStream`, ...)
 * @param schema                   optional schema supplied by the mapping definition
 */
case class FileSourceReadContext(
    spark: SparkSession,
    finalPath: String,
    isZipFile: Boolean,
    mappingSourceBinding: FileSystemSource,
    mappingJobSourceSettings: FileSystemSourceSettings,
    schema: Option[StructType]
)
