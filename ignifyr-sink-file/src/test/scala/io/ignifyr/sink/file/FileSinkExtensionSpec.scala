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
}
