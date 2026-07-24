package io.ignifyr.sink.fhir

import io.ignifyr.engine.model.FhirRepositorySinkSettings
import io.ignifyr.engine.spi.ExtensionRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Verifies the FHIR sink module is discovered through ServiceLoader when it is on the classpath
 * (no Docker required — this only inspects the extension registry).
 */
class FhirSinkExtensionSpec extends AnyFlatSpec with Matchers {

  "The FHIR sink extension" should "register the fhir-repository sink writer through ServiceLoader" in {
    val sink = ExtensionRegistry.sinkProviders.get(classOf[FhirRepositorySinkSettings])
    sink.map(_.id) shouldBe Some("fhir-repository")
  }

  it should "register the FHIR-server-backed terminology and identity service providers" in {
    ExtensionRegistry.terminologyServiceProviders.keySet should contain(classOf[FhirRepositorySinkSettings]: Class[_])
    ExtensionRegistry.identityServiceProviders.keySet should contain(classOf[FhirRepositorySinkSettings]: Class[_])
  }
}
