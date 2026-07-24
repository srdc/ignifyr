package io.ignifyr.connector.file

import com.typesafe.config.Config
import io.ignifyr.connector.file.format.FileFormatRegistry
import io.ignifyr.engine.data.read.BaseDataSourceReader
import io.ignifyr.engine.model.{
  FileSystemSource,
  FileSystemSourceSettings,
  MappingJobSourceSettings,
  MappingSourceBinding
}
import io.ignifyr.engine.spi.{IgnifyrExtension, SourceConnector}
import org.apache.spark.sql.SparkSession

/**
 * [[IgnifyrExtension]] for the community file connector: contributes the file-system source reader.
 * The set of file *formats* it reads (csv/tsv/parquet/...) is a separate, connector-local sub-SPI
 * discovered via the [[io.ignifyr.connector.file.format.FileFormatRegistry]], so enterprise format
 * modules can add JSON-source support without touching the engine. The file-system *sink* lives in
 * its own module, `ignifyr-sink-file`.
 */
class FileConnectorExtension extends IgnifyrExtension {

  override val id: String = "connector-file"

  /**
   * Force the source-format registry to materialize at engine startup (this runs during
   * `ExtensionRegistry` load), so a duplicate source-format registration fails fast here rather
   * than mid-job on first read — mirroring the engine's `ExtensionRegistry.init()`. Only reads
   * the classpath (ServiceLoader); does not touch the SparkSession.
   */
  override def initialize(config: Config): Unit = {
    FileFormatRegistry.sourceFormats
    ()
  }

  override def sourceConnectors: Seq[SourceConnector] = Seq(
    new SourceConnector {
      override val id: String = "file"
      override val bindingClass: Class[_ <: MappingSourceBinding] = classOf[FileSystemSource]
      override val settingsClass: Class[_ <: MappingJobSourceSettings] = classOf[FileSystemSourceSettings]
      override def createReader(spark: SparkSession): BaseDataSourceReader[_, _] = new FileDataSourceReader(spark)
    }
  )

  /**
   * Surface the connector-local file-format sub-registry (its own ServiceLoader) to `list-plugins`,
   * which the engine cannot introspect on its own — so the installed source formats (community
   * csv/tsv/parquet, plus any enterprise format-* modules on the classpath) still show up in the
   * plugin listing.
   */
  override def extraCapabilities: Seq[String] = Seq(
    s"file source formats: ${FileFormatRegistry.sourceFormats.keys.toSeq.sorted.mkString(", ")}"
  )
}
