package io.ignifyr.test.cli

import io.ignifyr.engine.model._
import io.ignifyr.engine.spi.ExtensionRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Community-edition separation guard.
 *
 * This module (`ignifyr-cli`) has exactly the community distribution on its classpath — the engine
 * plus `connector-sql` and `connector-file`, and none of the enterprise plugins. So the
 * `ExtensionRegistry` observed here is precisely what the shipped `ignifyr-engine-standalone.jar`
 * discovers via ServiceLoader. The suite asserts that enterprise capabilities and connectors are
 * ABSENT while the community ones are present. If someone accidentally pulls an enterprise module onto the
 * community distribution, these assertions fail.
 */
class CommunityEditionSeparationSpec extends AnyFlatSpec with Matchers {

  behavior of "The community edition classpath (engine + connector-sql + connector-file)"

  it should "register no enterprise execution capability (no streaming, no scheduling)" in {
    // Absent providers are what make startMappingJobStream / scheduleMappingJob raise the actionable
    // MissingCapabilityException instead of running.
    ExtensionRegistry.streaming shouldBe None
    ExtensionRegistry.scheduler shouldBe None
  }

  it should "register only the community source connectors" in {
    val sources = ExtensionRegistry.sourceConnectors.keySet
    sources should contain(classOf[SqlSource]: Class[_])
    sources should contain(classOf[FileSystemSource]: Class[_])
    // Enterprise source connectors must NOT be present on the community classpath.
    sources should not contain (classOf[KafkaSource]: Class[_])
    sources should not contain (classOf[FhirServerSource]: Class[_])
  }

  it should "register the community sinks (FHIR repository + file system)" in {
    val sinks = ExtensionRegistry.sinkProviders.keySet
    sinks should contain(classOf[FhirRepositorySinkSettings]: Class[_])
    sinks should contain(classOf[FileSystemSinkSettings]: Class[_])
  }

  it should "not expose enterprise CLI commands" in {
    // extract-redcap-schemas is provided by the enterprise ignifyr-redcap module.
    ExtensionRegistry.cliCommands.keySet should not contain "extract-redcap-schemas"
  }
}
