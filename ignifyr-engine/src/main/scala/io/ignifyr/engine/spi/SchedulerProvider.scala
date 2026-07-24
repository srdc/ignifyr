package io.ignifyr.engine.spi

import io.ignifyr.engine.execution.RunningJobRegistry
import io.ignifyr.engine.mapping.job.IFhirMappingJobManager
import io.ignifyr.engine.model.{
  BaseSchedulingSettings,
  FhirMappingJobExecution,
  SinkSettings,
  IdentityServiceSettings,
  MappingJobSourceSettings,
  TerminologyServiceSettings
}

/**
 * Supplies scheduled (cron-driven) batch execution as an installable capability. The community
 * engine runs batch jobs (and, via a [[StreamingExecutionProvider]], streaming jobs), but periodic
 * cron scheduling — the cron4j scheduler, the last-synchronization bookkeeping, and the registry of
 * scheduled executions — lives in the enterprise `ignifyr-runtime-scheduling` module. At most one
 * provider may be installed; a job carrying `schedulingSettings` with no provider fails with a
 * [[MissingCapabilityException]].
 *
 * The provider owns the scheduled-execution state (the counterpart of the engine's
 * [[RunningJobRegistry]], which tracks running batch/streaming jobs); the engine hands it the
 * running-job registry so a scheduled job's in-flight run surfaces as a running batch job and gets
 * the usual post-batch archiving.
 */
trait SchedulerProvider {

  /**
   * Validates the cron expression and schedules the given batch mapping job to run periodically,
   * synchronizing source data incrementally between runs (persisting the last sync time under
   * `ignifyrDbFolderPath`). Registers the scheduled execution (so it surfaces via [[isScheduled]] /
   * [[getScheduledExecutions]]) and starts the scheduler.
   *
   * @throws javax.ws.rs.BadRequestException  if the cron expression is invalid
   * @throws IllegalArgumentException         if `ignifyrDbFolderPath` is empty
   */
  def scheduleMappingJob(
      jobManager: IFhirMappingJobManager,
      runningJobRegistry: RunningJobRegistry,
      ignifyrDbFolderPath: String,
      mappingJobExecution: FhirMappingJobExecution,
      sourceSettings: Map[String, MappingJobSourceSettings],
      sinkSettings: SinkSettings,
      schedulingSettings: BaseSchedulingSettings,
      terminologyServiceSettings: Option[TerminologyServiceSettings],
      identityServiceSettings: Option[IdentityServiceSettings]
  ): Unit

  /** Whether the given job execution is currently scheduled. */
  def isScheduled(jobId: String, executionId: String): Boolean

  /** The scheduled execution ids for the given job (empty if none). */
  def getScheduledExecutions(jobId: String): Set[String]

  /**
   * Stops and removes a scheduled job execution, cancelling any in-flight run through the given
   * running-job registry.
   */
  def descheduleJobExecution(runningJobRegistry: RunningJobRegistry, jobId: String, executionId: String): Unit
}
