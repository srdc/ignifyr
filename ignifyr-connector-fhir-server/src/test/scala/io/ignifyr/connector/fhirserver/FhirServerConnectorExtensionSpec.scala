package io.ignifyr.connector.fhirserver

import io.ignifyr.engine.model.{FhirServerSource, FhirServerSourceSettings}
import io.ignifyr.engine.spi.ExtensionRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Verifies the FHIR-server connector is discovered through ServiceLoader when this module is on the
 * classpath (no Docker required — this only inspects the extension registry).
 */
class FhirServerConnectorExtensionSpec extends AnyFlatSpec with Matchers {

  "The FHIR-server connector extension" should "register a FhirServerSource connector through ServiceLoader" in {
    val connector = ExtensionRegistry.sourceConnectors.get(classOf[FhirServerSource])
    connector.map(_.id) shouldBe Some("fhir-server")
    connector.map(_.settingsClass) shouldBe Some(classOf[FhirServerSourceSettings])
  }
}
