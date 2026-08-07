package io.ignifyr.common.app

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * This module deliberately does not ship `version.properties` — the filtered file lives in
 * `ignifyr-engine` and `ignifyr-server`. So on this classpath the fallback is the whole behaviour, and
 * the fallback is what `/metadata` reports when a distribution forgets to filter the resource.
 */
class AppVersionTest extends AnyFlatSpec with Matchers {

  "getVersion" should "fall back to UNKNOWN when version.properties is not on the classpath" in {
    getClass.getClassLoader.getResource("version.properties") shouldBe null
    AppVersion.getVersion shouldBe "UNKNOWN"
  }
}
