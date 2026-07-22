package io.ignifyr.runtime.scheduling

import com.typesafe.scalalogging.Logger
import io.ignifyr.engine.Execution.actorSystem
import io.ignifyr.engine.execution.RunningJobRegistry
import io.ignifyr.engine.execution.log.ExecutionLogger
import io.ignifyr.engine.model.{FhirMappingJobExecution, FhirMappingJobResult}
import it.sauronsoftware.cron4j.{Scheduler, SchedulerListener, TaskExecutor}

/**
 * In-memory registry of cron-scheduled mapping-job executions — the scheduling counterpart of the
 * engine's [[RunningJobRegistry]] (which tracks running batch/streaming jobs). Lives in the
 * enterprise scheduling runtime with the cron4j [[Scheduler]] so the community engine carries no
 * scheduling dependency.
 *
 * On each cron fire, the attached [[SchedulerListener]] bridges the run into the engine's running-job
 * registry: it registers the launching run as a batch job (so it is visible/stoppable) and, on
 * completion, triggers the usual post-batch archiving + removal via
 * [[RunningJobRegistry.handleCompletedBatchJob]].
 */
class ScheduledJobRegistry {

  import actorSystem.dispatcher

  private val logger: Logger = Logger(this.getClass)

  // Keeps the scheduled jobs in the form of: jobId -> (executionId -> (Scheduler, execution))
  private val scheduledTasks
      : collection.mutable.Map[String, collection.mutable.Map[String, (Scheduler, FhirMappingJobExecution)]] =
    collection.mutable.Map[String, collection.mutable.Map[String, (Scheduler, FhirMappingJobExecution)]]()

  // When the actor system is terminated i.e. the system is shutdown, log every scheduled mapping job
  // as 'DESCHEDULED' (the engine's RunningJobRegistry logs running jobs as 'STOPPED').
  actorSystem.whenTerminated
    .map(_ => {
      scheduledTasks.values
        .flatMap(_.values.map(_._2))
        .foreach(execution => ExecutionLogger.logExecutionStatus(execution, FhirMappingJobResult.DESCHEDULED))
    })

  /**
   * Registers a scheduling job with the specified mapping job execution and scheduler, attaching a
   * listener that drives each cron fire into the given running-job registry.
   *
   * @param runningJobRegistry  Engine registry of running jobs, updated per cron fire.
   * @param mappingJobExecution The mapping job execution.
   * @param scheduler           The cron4j scheduler associated with the job execution.
   */
  def registerSchedulingJob(
      runningJobRegistry: RunningJobRegistry,
      mappingJobExecution: FhirMappingJobExecution,
      scheduler: Scheduler
  ): Unit = {
    // add it to the scheduledTasks map
    scheduledTasks
      .getOrElseUpdate(
        mappingJobExecution.jobId,
        collection.mutable.Map[String, (Scheduler, FhirMappingJobExecution)]()
      )
      .put(mappingJobExecution.id, (scheduler, mappingJobExecution))
    // log execution status as 'SCHEDULED'
    ExecutionLogger.logExecutionStatus(mappingJobExecution, FhirMappingJobResult.SCHEDULED)
    // add a scheduler listener to monitor task events
    scheduler.addSchedulerListener(new SchedulerListener {
      override def taskLaunching(executor: TaskExecutor): Unit = {
        runningJobRegistry.registerBatchJob(
          mappingJobExecution,
          None,
          s"Spark job for job: ${mappingJobExecution.jobId} mappingTasks: ${mappingJobExecution.mappingTasks.map(_.name).mkString(" ")}"
        )
      }

      override def taskSucceeded(executor: TaskExecutor): Unit = {
        runningJobRegistry.handleCompletedBatchJob(mappingJobExecution)
      }

      override def taskFailed(executor: TaskExecutor, exception: Throwable): Unit = {
        runningJobRegistry.handleCompletedBatchJob(mappingJobExecution)
      }
    })
  }

  /**
   * Deschedules a job execution.
   *
   * @param runningJobRegistry Engine registry of running jobs (to stop any in-flight run).
   * @param jobId              The ID of the job.
   * @param executionId        The ID of the execution.
   */
  def descheduleJobExecution(runningJobRegistry: RunningJobRegistry, jobId: String, executionId: String): Unit = {
    // TODO: We call this function but it does not actually stop the execution of a scheduled mappings jobs
    //  due to the fact that Spark can distribute the tasks into several threads and our setJobGroup/cancelJobGroup
    //  logic cannot work properly in this case
    // stop the job execution
    runningJobRegistry.stopJobExecution(jobId, executionId)
    // stop the scheduler for the specified job execution
    scheduledTasks(jobId)(executionId)._1.stop()
    logger.debug(s"Descheduled the mapping job with id: $jobId and execution: $executionId")
    // log execution status as 'DESCHEDULED'
    ExecutionLogger.logExecutionStatus(scheduledTasks(jobId)(executionId)._2, FhirMappingJobResult.DESCHEDULED)
    // remove the execution from the scheduledTask Map
    scheduledTasks(jobId).remove(executionId)
    // if there are no executions left for the job, remove the job from the map
    if (!scheduledTasks.contains(jobId)) {
      scheduledTasks.remove(jobId)
    }
  }

  /**
   * Checks if a job with the given execution ID is scheduled.
   *
   * @param jobId       The ID of the job
   * @param executionId The ID of the execution
   * @return True if the given job execution is scheduled, otherwise false.
   */
  def isScheduled(jobId: String, executionId: String): Boolean = {
    scheduledTasks.contains(jobId) && scheduledTasks(jobId).contains(executionId)
  }

  /**
   * Gets scheduled executions for the given job.
   *
   * @param jobId Identifier of the job
   * @return A set of execution ids
   */
  def getScheduledExecutions(jobId: String): Set[String] = {
    scheduledTasks.get(jobId).map(_.keySet).getOrElse(Set.empty).toSet
  }
}
