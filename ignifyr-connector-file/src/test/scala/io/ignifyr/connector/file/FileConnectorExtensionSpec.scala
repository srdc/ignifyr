package io.ignifyr.connector.file

import io.ignifyr.connector.file.format.{FileFormatRegistry, MissingFileFormatException}
import io.ignifyr.engine.model.{FileSystemSource, FileSystemSourceSettings, SourceContentTypes}
import io.ignifyr.engine.spi.ExtensionRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Verifies the file connector is discovered through ServiceLoader when this module is on the
 * classpath, and that its own format sub-SPI exposes the community source formats (no Docker —
 * this only inspects the registries). The JSON/NDJSON source is asserted in the enterprise format
 * module, so this spec only checks the formats that always ship here. The file *sink* and its
 * formats are asserted in `ignifyr-sink-file`'s registration spec.
 */
class FileConnectorExtensionSpec extends AnyFlatSpec with Matchers {

  "The file connector extension" should "register a FileSystemSource connector through ServiceLoader" in {
    val connector = ExtensionRegistry.sourceConnectors.get(classOf[FileSystemSource])
    connector.map(_.id) shouldBe Some("file")
    connector.map(_.settingsClass) shouldBe Some(classOf[FileSystemSourceSettings])
  }

  it should "discover the community source formats through the file format registry" in {
    FileFormatRegistry.sourceFormats.keySet should contain allElementsOf
      Seq(SourceContentTypes.CSV, SourceContentTypes.TSV, SourceContentTypes.PARQUET)
  }

  // The file formats live in the connector's own sub-registry, invisible to the engine; the
  // extension surfaces them to `list-plugins` through extraCapabilities.
  it should "summarize its source formats via extraCapabilities for list-plugins" in {
    val capabilities = new FileConnectorExtension().extraCapabilities.mkString("\n")
    capabilities should include("file source formats")
    capabilities should (include(SourceContentTypes.CSV) and include(SourceContentTypes.TSV) and
      include(SourceContentTypes.PARQUET))
  }

  // The JSON/NDJSON source format is enterprise (not on this module's classpath), so resolving it
  // here exercises the missing-format install-hint UX with the exact module coordinates.
  it should "raise a MissingFileFormatException naming the module for an uninstalled source format" in {
    val ex = intercept[MissingFileFormatException](FileFormatRegistry.sourceFormat(SourceContentTypes.JSON))
    ex.getMessage should include("com.pontegra.ignifyr:ignifyr-format-json")
  }

  // The counterpart of the missing-format path: a content type claimed by *two* installed handlers.
  // `FileConnectorExtension.initialize` force-materializes this registry so it surfaces at startup
  // rather than at first read. ServiceLoader input cannot be staged on this classpath, so the guard is
  // asserted on the indexing helper directly.
  it should "fail fast naming both handlers when two claim the same content type" in {
    val csv = FileFormatRegistry.sourceFormat(SourceContentTypes.CSV)
    val parquet = FileFormatRegistry.sourceFormat(SourceContentTypes.PARQUET)
    val ex = intercept[IllegalStateException] {
      FileFormatRegistry.indexUnique("file source format")(Seq("csv" -> csv, "csv" -> parquet))
    }
    ex.getMessage should include("Duplicate file source format registration")
    ex.getMessage should include("csv")
    ex.getMessage should (include(csv.getClass.getName) and include(parquet.getClass.getName))
  }

  it should "index one handler per content type" in {
    val csv = FileFormatRegistry.sourceFormat(SourceContentTypes.CSV)
    val parquet = FileFormatRegistry.sourceFormat(SourceContentTypes.PARQUET)
    FileFormatRegistry.indexUnique("file source format")(Seq("a" -> csv, "b" -> parquet)) shouldBe
      Map("a" -> csv, "b" -> parquet)
  }
}
