package io.ignifyr.test.engine.spi

import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.data.read.SourceHandler
import io.ignifyr.engine.model.{LocalFhirTerminologyServiceSettings, MappingJobSourceSettings, MappingSourceBinding}
import io.ignifyr.engine.spi.{ExtensionRegistry, MissingConnectorException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A source binding with no registered connector, used to exercise the missing-connector path. */
private case class UnregisteredSource() extends MappingSourceBinding {
  override def withPreprocessSql(preprocessSql: Option[String]): MappingSourceBinding = this
}

private case class UnregisteredSourceSettings(name: String = "unregistered", sourceUri: String = "urn:test")
    extends MappingJobSourceSettings

/**
 * Verifies the ServiceLoader-based extension registry: the engine core registers what ships in the
 * engine itself (the local terminology service and the built-in CLI commands — every concrete
 * source and sink lives in its own module), and a source binding with no installed connector fails
 * with an actionable message (rather than the job failing to parse, or a bare NotImplementedError as
 * before). Extracted sources/sinks (SQL, file, FHIR repository, ...) are asserted in those modules'
 * registration specs, since the engine's own test classpath does not include them.
 */
class ExtensionRegistrySpec extends AnyFlatSpec with Matchers {

  behavior of "ExtensionRegistry"

  it should "discover the engine core's own registrations through ServiceLoader" in {
    val terminologies = ExtensionRegistry.terminologyServiceProviders.keySet
    terminologies should contain(classOf[LocalFhirTerminologyServiceSettings]: Class[_])
  }

  it should "raise an actionable MissingConnectorException when no connector is registered for a source binding" in {
    val ex = intercept[MissingConnectorException] {
      SourceHandler.readSource(
        alias = "test",
        spark = IgnifyrConfig.sparkSession,
        mappingSource = UnregisteredSource(),
        mappingJobSourceSettings = UnregisteredSourceSettings(),
        schema = None
      )
    }
    ex.getMessage should include("No source reader registered")
    ex.getMessage should include("UnregisteredSource")
  }
}
