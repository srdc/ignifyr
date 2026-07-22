package io.ignifyr.runtime.scheduling

import io.ignifyr.engine.spi.{IgnifyrExtension, SchedulerProvider}

/**
 * Registers the scheduled (cron) execution capability with the engine via ServiceLoader.
 */
class SchedulingRuntimeExtension extends IgnifyrExtension {

  override val id: String = "runtime-scheduling"

  override def schedulerProvider: Option[SchedulerProvider] = Some(new Cron4jSchedulerProvider)
}
