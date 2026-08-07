package io.ignifyr.sink.file

import io.ignifyr.engine.model.{FileSystemSinkSettings, SinkContentTypes}
import io.ignifyr.engine.spi.ExtensionRegistry
import io.ignifyr.sink.file.format.{FileSinkFormatRegistry, MissingFileSinkFormatException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Verifies the file sink module is discovered through ServiceLoader when it is on the classpath,
 * and that its own format sub-SPI exposes the community sink formats (no Docker — this only
 * inspects the registries). The Delta sink format is asserted in the enterprise format module, so
 * this spec only checks the formats that always ship here.
 */
class FileSinkExtensionSpec extends AnyFlatSpec with Matchers {

  "The file sink extension" should "register a FileSystemSinkSettings sink provider through ServiceLoader" in {
    ExtensionRegistry.sinkProviders.get(classOf[FileSystemSinkSettings]).map(_.id) shouldBe Some("file")
  }

  it should "discover the community sink formats through the sink format registry" in {
    FileSinkFormatRegistry.sinkFormats.keySet should contain allElementsOf
      Seq(SinkContentTypes.NDJSON, SinkContentTypes.CSV, SinkContentTypes.PARQUET)
  }

  // The sink formats live in the module's own sub-registry, invisible to the engine; the extension
  // surfaces them to `list-plugins` through extraCapabilities.
  it should "summarize its sink formats via extraCapabilities for list-plugins" in {
    val capabilities = new FileSinkExtension().extraCapabilities.mkString("\n")
    capabilities should include("file sink formats")
    capabilities should (include(SinkContentTypes.NDJSON) and include(SinkContentTypes.CSV) and
      include(SinkContentTypes.PARQUET))
  }

  // The Delta sink format is enterprise (not on this module's classpath), so resolving it here
  // exercises the missing-format install-hint UX with the exact module coordinates.
  it should "raise a MissingFileSinkFormatException naming the module for an uninstalled sink format" in {
    val ex = intercept[MissingFileSinkFormatException](FileSinkFormatRegistry.sinkFormat(SinkContentTypes.DELTA_LAKE))
    ex.getMessage should include("com.pontegra.ignifyr:ignifyr-format-delta")
  }

  // The counterpart of the missing-format path: a content type claimed by *two* installed handlers.
  // `FileSinkExtension.initialize` force-materializes this registry so it surfaces at startup rather
  // than at first write. ServiceLoader input cannot be staged on this classpath, so the guard is
  // asserted on the indexing helper directly.
  it should "fail fast naming both handlers when two claim the same content type" in {
    val ndjson = FileSinkFormatRegistry.sinkFormat(SinkContentTypes.NDJSON)
    val parquet = FileSinkFormatRegistry.sinkFormat(SinkContentTypes.PARQUET)
    val ex = intercept[IllegalStateException] {
      FileSinkFormatRegistry.indexUnique("file sink format")(Seq("ndjson" -> ndjson, "ndjson" -> parquet))
    }
    ex.getMessage should include("Duplicate file sink format registration")
    ex.getMessage should include("ndjson")
    ex.getMessage should (include(ndjson.getClass.getName) and include(parquet.getClass.getName))
  }

  it should "index one handler per content type" in {
    val ndjson = FileSinkFormatRegistry.sinkFormat(SinkContentTypes.NDJSON)
    val parquet = FileSinkFormatRegistry.sinkFormat(SinkContentTypes.PARQUET)
    FileSinkFormatRegistry.indexUnique("file sink format")(Seq("a" -> ndjson, "b" -> parquet)) shouldBe
      Map("a" -> ndjson, "b" -> parquet)
  }
}
