package io.ignifyr.sink.file

import com.typesafe.config.Config
import io.ignifyr.engine.data.write.BaseSinkWriter
import io.ignifyr.engine.model.{FileSystemSinkSettings, SinkSettings}
import io.ignifyr.engine.spi.{IgnifyrExtension, SinkProvider}
import io.ignifyr.sink.file.format.FileSinkFormatRegistry

/**
 * [[IgnifyrExtension]] for the community file-system sink: contributes the file-system sink writer.
 * The set of output *formats* it handles (ndjson/csv/parquet/...) is a separate, module-local
 * sub-SPI discovered via the [[io.ignifyr.sink.file.format.FileSinkFormatRegistry]], so enterprise
 * format modules can add e.g. Delta output without touching the engine. The file-system *source*
 * lives in its own module, `ignifyr-connector-file`.
 */
class FileSinkExtension extends IgnifyrExtension {

  override val id: String = "sink-file"

  /**
   * Force the sink-format registry to materialize at engine startup (this runs during
   * `ExtensionRegistry` load), so a duplicate sink-format registration fails fast here rather than
   * mid-job on first write — mirroring the engine's `ExtensionRegistry.init()`. Only reads the
   * classpath (ServiceLoader); does not touch the SparkSession.
   */
  override def initialize(config: Config): Unit = {
    FileSinkFormatRegistry.sinkFormats
    ()
  }

  override def sinkProviders: Seq[SinkProvider] = Seq(
    new SinkProvider {
      override val id: String = "file"
      override val settingsClass: Class[_ <: SinkSettings] = classOf[FileSystemSinkSettings]
      override def createWriter(sinkSettings: SinkSettings): BaseSinkWriter =
        new FileSystemWriter(sinkSettings.asInstanceOf[FileSystemSinkSettings])
    }
  )

  /**
   * Surface the module-local sink-format sub-registry (its own ServiceLoader) to `list-plugins`,
   * which the engine cannot introspect on its own — so the installed sink formats (community
   * ndjson/csv/parquet, plus any enterprise format-* modules on the classpath) still show up in the
   * plugin listing.
   */
  override def extraCapabilities: Seq[String] = Seq(
    s"file sink formats: ${FileSinkFormatRegistry.sinkFormats.keys.toSeq.sorted.mkString(", ")}"
  )
}
