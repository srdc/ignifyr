package io.ignifyr.connector.file

import io.ignifyr.engine.data.read.BaseDataSourceReader
import io.ignifyr.engine.data.write.BaseFhirWriter
import io.ignifyr.engine.model.{
  FhirSinkSettings,
  FileSystemSinkSettings,
  FileSystemSource,
  FileSystemSourceSettings,
  MappingJobSourceSettings,
  MappingSourceBinding
}
import io.ignifyr.engine.spi.{IgnifyrExtension, SinkProvider, SourceConnector}
import org.apache.spark.sql.SparkSession

/**
 * [[IgnifyrExtension]] for the community file connector: contributes the file-system source reader
 * and the file-system sink writer. The set of file *formats* those handle (csv/tsv/parquet/... source,
 * ndjson/csv/parquet/... sink) is a separate, connector-local sub-SPI discovered via the
 * [[io.ignifyr.connector.file.format.FileFormatRegistry]], so enterprise format modules can add
 * JSON-source / Delta-sink support without touching the engine.
 */
class FileConnectorExtension extends IgnifyrExtension {

  override val id: String = "connector-file"

  override def sourceConnectors: Seq[SourceConnector] = Seq(
    new SourceConnector {
      override val id: String = "file"
      override val bindingClass: Class[_ <: MappingSourceBinding] = classOf[FileSystemSource]
      override val settingsClass: Class[_ <: MappingJobSourceSettings] = classOf[FileSystemSourceSettings]
      override def createReader(spark: SparkSession): BaseDataSourceReader[_, _] = new FileDataSourceReader(spark)
    }
  )

  override def sinkProviders: Seq[SinkProvider] = Seq(
    new SinkProvider {
      override val id: String = "file"
      override val settingsClass: Class[_ <: FhirSinkSettings] = classOf[FileSystemSinkSettings]
      override def createWriter(sinkSettings: FhirSinkSettings): BaseFhirWriter =
        new FileSystemWriter(sinkSettings.asInstanceOf[FileSystemSinkSettings])
    }
  )
}
