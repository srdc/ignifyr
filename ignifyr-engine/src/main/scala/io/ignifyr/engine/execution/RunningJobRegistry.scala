package io.ignifyr.engine.execution

import com.typesafe.scalalogging.Logger
import io.ignifyr.engine.Execution.actorSystem.dispatcher
import io.ignifyr.engine.model.{FhirMappingJobExecution, FhirMappingJobResult}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException}

import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import io.ignifyr.engine.Execution.actorSystem
import io.ignifyr.engine.execution.log.ExecutionLogger
import io.ignifyr.engine.execution.processing.FileStreamInputArchiver
import io.ignifyr.engine.spi.ExtensionRegistry

/**
 * Execution manager that keeps track of running and scheduled mapping tasks in-memory.
 * This registry is designed to maintain the execution status of both Streaming and Batch mapping jobs.
 *
 * For Streaming Jobs:
 * - Each task's execution status is tracked individually.
 *
 * For Batch Jobs:
 * - The registry maintains the overall execution status of the Batch job.
 * - Individual statuses of tasks within a batch job are not maintained in the registry due to the nature of batch processing.
 *
 * Scheduled (cron) executions are tracked separately by the enterprise `ignifyr-runtime-scheduling`
 * module (`SchedulerProvider`), which drives running batch jobs into this registry through
 * [[registerBatchJob]] / [[handleCompletedBatchJob]] — so the engine carries no scheduling dependency.
 */
class RunningJobRegistry(spark: SparkSession) {
  // Keeps active executions in the form of: jobId -> (executionId -> execution)
  private val runningTasks: collection.mutable.Map[String, collection.mutable.Map[String, FhirMappingJobExecution]] =
    collection.mutable.Map[String, collection.mutable.Map[String, FhirMappingJobExecution]]()

  // Dedicated execution context for blocking streaming jobs
  private val streamingTaskExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool)

  private val logger: Logger = Logger(this.getClass)

  /**
   * When the actor system is terminated i.e., the system is shutdown, log the status of running mapping jobs
   * as 'STOPPED'. Scheduled mapping jobs are logged as 'DESCHEDULED' by the scheduling runtime module.
   */
  actorSystem.whenTerminated
    .map(_ => {
      // iterate over all running tasks and log each one as 'STOPPED'
      runningTasks.values
        .flatMap(_.values)
        .foreach(execution => {
          // log execution status as 'STOPPED'
          ExecutionLogger.logExecutionStatus(execution, FhirMappingJobResult.STOPPED)
        })
    })

  /**
   * Caches a [[FhirMappingJobExecution]] for an individual mapping task
   *
   * @param execution            Execution containing the mapping tasks
   * @param mappingTaskName      Specific name which the [[StreamingQuery]] is associated to
   * @param streamingQueryFuture Future for the [[StreamingQuery]]
   * @return
   */
  def registerStreamingQuery(
      execution: FhirMappingJobExecution,
      mappingTaskName: String,
      streamingQueryFuture: Future[StreamingQuery]
  ): Future[Unit] = {
    Future {
      // If there is an error in the streaming query execution, call 'stopMappingExecution' function,
      // which is responsible for removing it from the registry. Without that, the registry might contain incorrect
      // information such as indicating that the job is still running when it has encountered an error.
      streamingQueryFuture.recover(_ => stopMappingExecution(execution.jobId, execution.id, mappingTaskName))
      // Wait for the initial Future to be resolved
      val streamingQuery: StreamingQuery = Await.result(streamingQueryFuture, Duration.Inf)
      val jobId: String = execution.jobId
      val executionId: String = execution.id

      // Multiple threads can update the global task map. So, updates are synchronized.
      val updatedExecution = runningTasks.synchronized {
        // Update the execution map
        val executionMap: collection.mutable.Map[String, FhirMappingJobExecution] =
          runningTasks.getOrElseUpdate(jobId, collection.mutable.Map[String, FhirMappingJobExecution]())
        val updatedExecution: FhirMappingJobExecution = executionMap.get(executionId) match {
          case None =>
            execution.copy(jobGroupIdOrStreamingQuery =
              Some(Right(collection.mutable.Map(mappingTaskName -> streamingQuery)))
            )
          case Some(execution) =>
            execution.copy(jobGroupIdOrStreamingQuery =
              Some(Right(execution.getStreamingQueryMap() + (mappingTaskName -> streamingQuery)))
            )
        }
        executionMap.put(executionId, updatedExecution)
        updatedExecution
      }
      logger.debug(
        s"Streaming query for execution: $executionId, mappingTaskName: $mappingTaskName has been registered"
      )

      try {
        // wait for StreamingQuery to terminate
        updatedExecution.getStreamingQuery(mappingTaskName).awaitTermination()
      } catch {
        case exception: StreamingQueryException =>
          // Ask connector-provided descriptors to translate the failure into a clearer message
          // (e.g. Kafka's "unknown topic" error naming the missing topics). Fall back to the raw
          // exception if no installed connector recognizes it — the engine stays connector-agnostic.
          val describedError: Throwable = ExtensionRegistry.sourceFailureDescriptors
            .flatMap(_.describeStreamingFailure(exception, execution, mappingTaskName))
            .headOption
            .getOrElse(exception)
          ExecutionLogger.logExecutionStatus(
            execution,
            FhirMappingJobResult.FAILURE,
            Some(mappingTaskName),
            Some(describedError),
            isChunkResult = false
          )
      } finally {
        // Remove the mapping execution from the running tasks after the query is terminated
        stopMappingExecution(jobId, executionId, mappingTaskName)
      }

      // Use the dedicated ExecutionContext for streaming jobs
    }(streamingTaskExecutionContext)
  }

  // TODO: Improve cancellation of running batch mapping jobs.
  //  Currently, we rely on setJobGroup and cancelJobGroup functions in Spark to cancel tasks associated with a mapping job.
  //  However, due to Spark's task distribution across multiple threads, using cancelJobGroup may not cancel all tasks,
  //  especially in the case of scheduled mapping jobs because setJobGroup only works for the Spark tasks started in the same thread.
  //  To resolve this issue, we need to assign a unique Spark job group id at the start of mapping job execution, before registering it with the RunningJobRegistry.
  //  We should call setJobGroup function before Spark tasks begin execution. The ideal location for this call seems to be the readSourceExecuteAndWriteInChunks function,
  //  although thorough testing is required to ensure its effectiveness.
  //  UPDATE: Although I set the job group id of each thread to the same value, cancelJobGroup does not work as expected
  //  because it only cancels the active jobs in Spark 3.5 not the future submitted jobs.
  //  In the main branch of Spark, this method can take a parameter named cancelFutureJobs that can resolve our problem.
  //  Please see https://github.com/apache/spark/blob/33a153a6bbcba0d9b2ab20404c7d3b6db86d7b4a/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L1108
  /**
   * Caches a batch job. This method sets the Spark job group id for further referencing (e.g. cancelling the Spark jobs via the job group).
   * Spark job group manages job groups per different threads. This practically means that for each mapping execution request initiated by a REST call would have a different job group.
   * We utilize jobFuture to handle the completion of the job, however, for scheduled mapping jobs, we do not have such a future. Its completion is handled by the scheduling runtime module's
   * scheduler listener, which calls [[handleCompletedBatchJob]] directly.
   *
   * @param execution      Execution representing the batch job.
   * @param jobFuture      Unified Future to yield the completion of the mapping tasks (Optional since scheduling jobs do not have a future).
   * @param jobDescription Job description to be used by Spark. Spark uses it for reporting purposes.
   * @return A future that completes once the mapping tasks have finished **and** the post-completion
   *         handling ([[handleCompletedBatchJob]] — input archiving plus deregistration) has run.
   *         Await this, not `jobFuture`, when the caller needs archiving to be done; an already
   *         completed future is returned when there is no `jobFuture` to hang off.
   */
  def registerBatchJob(
      execution: FhirMappingJobExecution,
      jobFuture: Option[Future[Unit]],
      jobDescription: String = ""
  ): Future[Unit] = {
    val jobGroup: String = setSparkJobGroup(jobDescription)
    val executionWithJobGroupId = execution.copy(jobGroupIdOrStreamingQuery = Some(Left(jobGroup)))
    val jobId: String = executionWithJobGroupId.jobId
    val executionId: String = executionWithJobGroupId.id

    runningTasks.synchronized {
      runningTasks
        .getOrElseUpdate(jobId, collection.mutable.Map[String, FhirMappingJobExecution]())
        .put(executionId, executionWithJobGroupId)

      logger.debug(s"Batch job for execution: $executionId has been registered with spark job group id: $jobGroup")
    }
    // Archive the processed inputs and drop the execution entry once the job finishes. `andThen`
    // rather than `onComplete` so the returned future completes only AFTER that handling has run —
    // `onComplete` merely schedules it, which leaves a caller that awaits the raw job future racing
    // against the archiving. That race is live for the one-shot batch CLI, which calls
    // System.exit(0) as soon as the await returns.
    jobFuture match {
      case Some(future) => future.andThen { case _ => handleCompletedBatchJob(execution) }
      case None => Future.successful(())
    }
  }

  /**
   * Stops all [[StreamingQuery]]s associated with the specified execution.
   *
   * @param jobId       Identifier of the job associated with the execution
   * @param executionId Identifier of the execution to be stopped
   */
  def stopJobExecution(jobId: String, executionId: String): Unit = {
    removeExecutionFromRunningTasks(jobId, executionId) match {
      case None => // Nothing to do
      case Some(execution) =>
        execution.jobGroupIdOrStreamingQuery.get match {
          // For batch jobs, we cancel the job group.
          case Left(sparkJobGroup) =>
            spark.sparkContext.cancelJobGroup(sparkJobGroup)
            logger.debug(s"Canceled Spark job group with id: $sparkJobGroup")

          // For streaming jobs, we terminate the streaming queries one by one
          case Right(queryMap) =>
            queryMap.foreach(queryEntry => {
              queryEntry._2.stop()
              logger.debug(s"Stopped streaming query for mapping: ${queryEntry._1}")
              // Log each mapping task as stopped
              ExecutionLogger.logExecutionStatus(execution, FhirMappingJobResult.STOPPED, Some(queryEntry._1))
            })
        }
    }
  }

  /**
   * Stops all the executions of a mapping job with the specified jobId.
   *
   * @param jobId The identifier of the mapping job for which executions should be stopped.
   */
  def stopJobExecutions(jobId: String): Unit = {
    runningTasks.get(jobId) match {
      case None => // No running tasks for the specified jobId, nothing to do
      case Some(executionsMap) =>
        // Stop each individual execution associated with the jobId
        executionsMap.values.toSeq.foreach(execution => removeExecutionFromRunningTasks(jobId, execution.id))
    }
  }

  /**
   * Stops the [[StreamingQuery]] associated with an individual mapping task
   *
   * @param jobId            Identified of the associated with the execution
   * @param executionId      Identifier of the job containing the mapping
   * @param mappingTaskName  Name of the mappingTask
   */
  def stopMappingExecution(jobId: String, executionId: String, mappingTaskName: String): Unit = {
    removeMappingExecutionFromRunningTasks(jobId, executionId, mappingTaskName) match {
      case None => // Nothing to do
      case Some(result) =>
        result match {
          case Left(sparkJobGroup) => spark.sparkContext.cancelJobGroup(sparkJobGroup)
          case Right(streamingQuery) => streamingQuery.stop()
        }
    }
  }

  /**
   * Removes the entry from the running tasks map for the given job and execution. If the removed entry is the last one for the given job,
   * the complete job entry is also removed.
   *
   * @param jobId       Identifier of the job
   * @param executionId Identifier of the execution
   * @return Returns the removed mapping entry if at all (executionId -> execution), or None
   */
  private def removeExecutionFromRunningTasks(jobId: String, executionId: String): Option[FhirMappingJobExecution] = {
    runningTasks.synchronized {
      runningTasks.get(jobId) match {
        case Some(jobMapping) if jobMapping.contains(executionId) =>
          val removedExecutionEntry = jobMapping.remove(executionId)
          // Remove the job mappings completely if it is empty
          if (runningTasks(jobId).isEmpty) {
            runningTasks.remove(jobId)
          }
          removedExecutionEntry
        case _ => None
      }
    }
  }

  /**
   * Removes the entry from the running tasks map for the given job, execution and mapping. If the removed entry is the last one for the given execution, the execution itself is also removed.
   * Furthermore, if the execution is the last one for the given job, the job entry is also removed.
   *
   * @param jobId           Identifier of the job
   * @param executionId     Identifier of the execution
   * @param mappingTaskName Name of the mappingTask
   * @return Returns the removed mapping entry if at all or None
   */
  private def removeMappingExecutionFromRunningTasks(
      jobId: String,
      executionId: String,
      mappingTaskName: String
  ): Option[Either[String, StreamingQuery]] = {
    runningTasks.synchronized {
      runningTasks.get(jobId) match {
        case Some(jobMapping) if jobMapping.contains(executionId) =>
          val execution: FhirMappingJobExecution = jobMapping(executionId)
          var removedMappingEntry: Option[Either[String, StreamingQuery]] = None
          // If it is a batch job do nothing but warn user about the situation
          if (!execution.isStreamingJob) {
            logger.warn(
              s"Execution with $jobId: $jobId, executionId: $executionId, mappingTaskName: $mappingTaskName won't be stopped with a specific mapping as this is a batch job." +
                s"Stop execution by providing only the jobId and executionId"
            )

            // Streaming query
          } else {
            if (execution.getStreamingQueryMap().contains(mappingTaskName)) {
              removedMappingEntry = Some(Right(execution.getStreamingQueryMap().remove(mappingTaskName).get))
              // Remove the execution mappings completely if it is empty
              if (execution.getStreamingQueryMap().isEmpty) {
                jobMapping.remove(executionId)
                // Remove the job mappings completely if it is empty
                if (runningTasks(jobId).isEmpty) {
                  runningTasks.remove(jobId)
                }
              }
            }
          }
          removedMappingEntry
        case _ => None
      }
    }
  }

  /**
   * Checks existence of execution for a job or a mapping task
   *
   * @param jobId           Identifier of the job associated with the execution
   * @param executionId     Identifier of the execution to be stopped
   * @param mappingTaskName Name of the mappingTask representing the mapping task being executed
   * @return
   */
  def executionExists(jobId: String, executionId: String, mappingTaskName: Option[String]): Boolean = {
    if (runningTasks.contains(jobId) && runningTasks(jobId).contains(executionId)) {
      mappingTaskName match {
        case None => true // We know we have an execution at this point
        case Some(name) =>
          // For streaming jobs, we check whether there is a streaming query for the given mappingTask
          if (runningTasks(jobId)(executionId).isStreamingJob) {
            runningTasks(jobId)(executionId).getStreamingQueryMap().contains(name)

            // For batch jobs, we don't differentiate mapping tasks. So, returning true directly (which indicates that the job execution is in progress)
          } else {
            true
          }
      }
    } else {
      false
    }
  }

  /**
   * Checks whether a mapping job with the specified jobId is currently running.
   *
   * This method determines if a mapping job is running by checking if there are
   * any active executions associated with the specified jobId.
   *
   * @param jobId The identifier of the mapping job to be checked for running status.
   * @return `true` if the specified mapping job is running, otherwise `false`.
   */
  def isJobRunning(jobId: String): Boolean = {
    runningTasks.contains(jobId)
  }

  /**
   * Gets running executions for the given job
   *
   * @param jobId Identifier of the job
   * @return A set of execution ids
   */
  def getRunningExecutions(jobId: String): Set[String] = {
    runningTasks.get(jobId).map(_.keySet).getOrElse(Set.empty).toSet
  }

  /**
   * Converts the running task map into a structure as follows: (jobId -> sequence of (executionId -> sequence of mappingTask names))
   *
   * @return
   */
  def getRunningExecutions(): Map[String, Seq[(String, Seq[String])]] = {
    runningTasks
      .map(entry =>
        entry._1 -> // jobId
          entry._2 // a map in the form of (executionId -> FhirMappingJobExecution)
            .map(executionMappings =>
              (executionMappings._1, executionMappings._2.mappingTasks.map(_.name))
            ) // (executionId -> sequence of mappingTask names)
            .toSeq
      )
      .toMap
  }

  /**
   * Returns [[FhirMappingJobExecution]]s for all the running executions
   *
   * @return
   */
  def getRunningExecutionsWithCompleteMetadata(): Seq[FhirMappingJobExecution] = {
    runningTasks
      .flatMap(_._2.values) // concatenate executions of all jobs
      .toSeq
  }

  /**
   * Sets the Spark job group for the active thread, which is used to uniquely identify and manage
   * batch jobs. This is particularly useful for closing mapping tasks associated with a specific job.
   *
   * @param description Description for the job group
   * @return The generated job group id
   */
  private def setSparkJobGroup(description: String = ""): String = {
    val newJobGroup: String = UUID.randomUUID().toString
    spark.sparkContext.setJobGroup(newJobGroup, description, true)
    newJobGroup
  }

  /**
   * Handles the completion of a batch job execution.
   * This method runs archiving manually for the batch job and removes the execution from the list of running tasks.
   * Public because the scheduling runtime module's scheduler listener calls it when a scheduled batch run finishes.
   *
   * @param execution The execution of the batch job to handle.
   */
  def handleCompletedBatchJob(execution: FhirMappingJobExecution): Unit = {
    FileStreamInputArchiver.applyArchivingOnBatchJob(execution)
    removeExecutionFromRunningTasks(execution.jobId, execution.id)
  }
}
