package io.ignifyr.sink.omop

import io.ignifyr.engine.spi.ExtensionRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Verifies the OMOP sink skeleton is discovered through ServiceLoader when it is on the classpath
 * (no Docker required — this only inspects the extension registry). The extension contributes
 * nothing yet; contribution assertions arrive with the map-to-OMOP implementation.
 */
class OmopSinkExtensionSpec extends AnyFlatSpec with Matchers {

  "The OMOP sink extension" should "be discovered through ServiceLoader" in {
    ExtensionRegistry.extensions.map(_.id) should contain("sink-omop")
  }
}
