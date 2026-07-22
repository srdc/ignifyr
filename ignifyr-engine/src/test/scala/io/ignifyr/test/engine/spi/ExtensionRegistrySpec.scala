package io.ignifyr.test.engine.spi

import io.ignifyr.IgnifyrTestSpec
import io.ignifyr.engine.data.read.SourceHandler
import io.ignifyr.engine.model.{
  FhirRepositorySinkSettings,
  FileSystemSinkSettings,
  FileSystemSource,
  MappingJobSourceSettings,
  MappingSourceBinding
}
import io.ignifyr.engine.spi.{ExtensionRegistry, MissingConnectorException}
import org.scalatest.flatspec.AnyFlatSpec

/** A source binding with no registered connector, used to exercise the missing-connector path. */
private case class UnregisteredSource() extends MappingSourceBinding {
  override def withPreprocessSql(preprocessSql: Option[String]): MappingSourceBinding = this
}

private case class UnregisteredSourceSettings(name: String = "unregistered", sourceUri: String = "urn:test")
    extends MappingJobSourceSettings

/**
 * Verifies the ServiceLoader-based extension registry: the community core registers its connectors
 * and sinks, and a source binding with no installed connector fails with an actionable message
 * (rather than the job failing to parse, or a bare NotImplementedError as before).
 */
class ExtensionRegistrySpec extends AnyFlatSpec with IgnifyrTestSpec {

  behavior of "ExtensionRegistry"

  it should "discover the in-engine core source connectors through ServiceLoader" in {
    // Source connectors extracted into their own modules (e.g. SQL) are asserted in those modules;
    // these are the ones still registered by CoreExtension on the engine's own classpath.
    val bindings = ExtensionRegistry.sourceConnectors.keySet
    bindings should contain(classOf[FileSystemSource]: Class[_])
  }

  it should "discover the community core sink providers through ServiceLoader" in {
    val sinks = ExtensionRegistry.sinkProviders.keySet
    sinks should contain(classOf[FhirRepositorySinkSettings]: Class[_])
    sinks should contain(classOf[FileSystemSinkSettings]: Class[_])
  }

  it should "raise an actionable MissingConnectorException when no connector is registered for a source binding" in {
    val ex = intercept[MissingConnectorException] {
      SourceHandler.readSource(
        alias = "test",
        spark = sparkSession,
        mappingSource = UnregisteredSource(),
        mappingJobSourceSettings = UnregisteredSourceSettings(),
        schema = None
      )
    }
    ex.getMessage should include("No source reader registered")
    ex.getMessage should include("UnregisteredSource")
  }
}
