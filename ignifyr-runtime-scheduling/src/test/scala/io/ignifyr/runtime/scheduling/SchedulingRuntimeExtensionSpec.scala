package io.ignifyr.runtime.scheduling

import io.ignifyr.engine.spi.ExtensionRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Lightweight, no-Docker smoke for the scheduling runtime: the [[SchedulingRuntimeExtension]] is
 * discovered through ServiceLoader so the engine exposes a scheduling capability, and a fresh
 * provider reports no scheduled executions. The full cron/SQL/onFHIR behavioural test (the parked
 * `SchedulingTest`) is re-homed here with the shared testkit, once fixtures are classpath-portable.
 */
class SchedulingRuntimeExtensionSpec extends AnyFlatSpec with Matchers {

  behavior of "SchedulingRuntimeExtension"

  it should "register a scheduling execution provider with the engine through ServiceLoader" in {
    ExtensionRegistry.scheduler shouldBe defined
  }

  it should "report no scheduled executions for an unknown job on a fresh registry" in {
    val provider = new Cron4jSchedulerProvider
    provider.isScheduled("no-such-job", "no-such-execution") shouldBe false
    provider.getScheduledExecutions("no-such-job") shouldBe empty
  }
}
