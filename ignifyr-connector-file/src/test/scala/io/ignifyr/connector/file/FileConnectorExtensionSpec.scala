package io.ignifyr.connector.file

import io.ignifyr.connector.file.format.{FileFormatRegistry, MissingFileFormatException}
import io.ignifyr.engine.model.{
  FileSystemSinkSettings,
  FileSystemSource,
  FileSystemSourceSettings,
  SinkContentTypes,
  SourceContentTypes
}
import io.ignifyr.engine.spi.ExtensionRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Verifies the file connector is discovered through ServiceLoader when this module is on the
 * classpath, and that its own format sub-SPI exposes the community source/sink formats (no Docker —
 * this only inspects the registries). JSON/NDJSON source and the Delta sink are asserted in the
 * enterprise format-* modules, so this spec only checks the formats that always ship here.
 */
class FileConnectorExtensionSpec extends AnyFlatSpec with Matchers {

  "The file connector extension" should "register a FileSystemSource connector through ServiceLoader" in {
    val connector = ExtensionRegistry.sourceConnectors.get(classOf[FileSystemSource])
    connector.map(_.id) shouldBe Some("file")
    connector.map(_.settingsClass) shouldBe Some(classOf[FileSystemSourceSettings])
  }

  it should "register a FileSystemSinkSettings sink provider through ServiceLoader" in {
    ExtensionRegistry.sinkProviders.get(classOf[FileSystemSinkSettings]).map(_.id) shouldBe Some("file")
  }

  it should "discover the community source formats through the file format registry" in {
    FileFormatRegistry.sourceFormats.keySet should contain allElementsOf
      Seq(SourceContentTypes.CSV, SourceContentTypes.TSV, SourceContentTypes.PARQUET)
  }

  it should "discover the community sink formats through the file format registry" in {
    FileFormatRegistry.sinkFormats.keySet should contain allElementsOf
      Seq(SinkContentTypes.NDJSON, SinkContentTypes.CSV, SinkContentTypes.PARQUET)
  }

  // The file formats live in the connector's own sub-registry, invisible to the engine; the
  // extension surfaces them to `list-plugins` through extraCapabilities.
  it should "summarize its source and sink formats via extraCapabilities for list-plugins" in {
    val capabilities = new FileConnectorExtension().extraCapabilities.mkString("\n")
    capabilities should include("file source formats")
    capabilities should (include(SourceContentTypes.CSV) and include(SourceContentTypes.TSV) and
      include(SourceContentTypes.PARQUET))
    capabilities should include("file sink formats")
    capabilities should (include(SinkContentTypes.NDJSON) and include(SinkContentTypes.PARQUET))
  }

  // The JSON/NDJSON source and Delta sink formats are enterprise (not on this module's classpath), so
  // resolving them here exercises the missing-format install-hint UX with the exact module coordinates.
  it should "raise a MissingFileFormatException naming the module for an uninstalled source format" in {
    val ex = intercept[MissingFileFormatException](FileFormatRegistry.sourceFormat(SourceContentTypes.JSON))
    ex.getMessage should include("com.pontegra.ignifyr:ignifyr-format-json")
  }

  it should "raise a MissingFileFormatException naming the module for an uninstalled sink format" in {
    val ex = intercept[MissingFileFormatException](FileFormatRegistry.sinkFormat(SinkContentTypes.DELTA_LAKE))
    ex.getMessage should include("com.pontegra.ignifyr:ignifyr-format-delta")
  }
}
