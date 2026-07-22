package io.ignifyr.runtime.scheduling

import com.typesafe.scalalogging.Logger
import io.ignifyr.engine.execution.RunningJobRegistry
import io.ignifyr.engine.mapping.job.IFhirMappingJobManager
import io.ignifyr.engine.model.{
  BaseSchedulingSettings,
  FhirMappingJobExecution,
  FhirSinkSettings,
  IdentityServiceSettings,
  MappingJobSourceSettings,
  SchedulingSettings,
  SQLSchedulingSettings,
  TerminologyServiceSettings
}
import io.ignifyr.engine.spi.SchedulerProvider
import it.sauronsoftware.cron4j.{Scheduler, SchedulingPattern}

import java.io.{File, FileNotFoundException, FileWriter}
import java.net.URI
import java.nio.file.Paths
import java.time.{Instant, LocalDateTime, ZoneOffset}
import javax.ws.rs.BadRequestException
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source

/**
 * cron4j-backed [[SchedulerProvider]]: schedules batch mapping jobs on a cron expression, keeps a
 * per-job last-synchronization time (under `<ignifyrDbFolderPath>/scheduler/<jobId>.txt`) so each
 * run only maps data added since the previous run, and tracks scheduled executions in a
 * [[ScheduledJobRegistry]]. This is the body that previously lived in `FhirMappingJobManager`
 * (`scheduleMappingJob`/`runnableMappingJob`/`getScheduledTimeRange`) plus the old `MappingJobScheduler`.
 */
class Cron4jSchedulerProvider extends SchedulerProvider {

  import io.ignifyr.engine.Execution.actorSystem.dispatcher

  private val logger: Logger = Logger(this.getClass)

  private val scheduledJobRegistry: ScheduledJobRegistry = new ScheduledJobRegistry

  override def scheduleMappingJob(
      jobManager: IFhirMappingJobManager,
      runningJobRegistry: RunningJobRegistry,
      ignifyrDbFolderPath: String,
      mappingJobExecution: FhirMappingJobExecution,
      sourceSettings: Map[String, MappingJobSourceSettings],
      sinkSettings: FhirSinkSettings,
      schedulingSettings: BaseSchedulingSettings,
      terminologyServiceSettings: Option[TerminologyServiceSettings],
      identityServiceSettings: Option[IdentityServiceSettings]
  ): Unit = {
    if (ignifyrDbFolderPath.isEmpty) {
      throw new IllegalArgumentException(
        "A scheduled mapping job was submitted, but ignifyr.db is not configured."
      )
    }
    // validate the cron expression
    if (!SchedulingPattern.validate(schedulingSettings.cronExpression)) {
      throw new BadRequestException(s"'${schedulingSettings.cronExpression}' is not a valid cron expression.")
    }
    // Folder that acts as the scheduler's state store for last-synchronization times.
    val folderUri: URI = Paths.get(ignifyrDbFolderPath, "scheduler").toUri
    // find the start time for SQL data sources
    val startTime: LocalDateTime = schedulingSettings match {
      case SQLSchedulingSettings(_, initialTime) =>
        if (initialTime.isEmpty) {
          logger.info(
            s"initialTime is not specified in the mappingJob. I will sync all the data from midnight, January 1, 1970 to the next run time."
          )
          Instant.ofEpochMilli(0L).atOffset(ZoneOffset.UTC).toLocalDateTime
        } else {
          LocalDateTime.parse(initialTime.get)
        }
      case SchedulingSettings(_) =>
        Instant.ofEpochMilli(0L).atOffset(ZoneOffset.UTC).toLocalDateTime
    }
    // Schedule a task
    val scheduler = new Scheduler()
    scheduler.schedule(
      schedulingSettings.cronExpression,
      new Runnable() {
        override def run(): Unit = {
          val scheduledJob = runnableMappingJob(
            jobManager,
            folderUri,
            mappingJobExecution,
            startTime,
            sourceSettings,
            sinkSettings,
            terminologyServiceSettings,
            identityServiceSettings,
            schedulingSettings
          )
          Await.result(scheduledJob, Duration.Inf)
        }
      }
    )
    // Register the scheduled execution (attaches the listener bridging cron fires into the running-job
    // registry and logs SCHEDULED), then start the scheduler.
    scheduledJobRegistry.registerSchedulingJob(runningJobRegistry, mappingJobExecution, scheduler)
    scheduler.start()
  }

  override def isScheduled(jobId: String, executionId: String): Boolean =
    scheduledJobRegistry.isScheduled(jobId, executionId)

  override def getScheduledExecutions(jobId: String): Set[String] =
    scheduledJobRegistry.getScheduledExecutions(jobId)

  override def descheduleJobExecution(
      runningJobRegistry: RunningJobRegistry,
      jobId: String,
      executionId: String
  ): Unit =
    scheduledJobRegistry.descheduleJobExecution(runningJobRegistry, jobId, executionId)

  /**
   * Runnable body for a scheduled periodic mapping job: computes the incremental time range from the
   * last-sync file, executes the batch job over it, then records the new sync time.
   */
  private def runnableMappingJob(
      jobManager: IFhirMappingJobManager,
      folderUri: URI,
      mappingJobExecution: FhirMappingJobExecution,
      startTime: LocalDateTime,
      sourceSettings: Map[String, MappingJobSourceSettings],
      sinkSettings: FhirSinkSettings,
      terminologyServiceSettings: Option[TerminologyServiceSettings],
      identityServiceSettings: Option[IdentityServiceSettings],
      schedulingSettings: BaseSchedulingSettings
  ): Future[Unit] = {
    val timeRange = getScheduledTimeRange(mappingJobExecution.jobId, folderUri, startTime)
    logger.info(s"Running scheduled job with the expression: ${schedulingSettings.cronExpression}")
    logger.info(s"Synchronizing data between ${timeRange._1} and ${timeRange._2}")
    jobManager
      .executeMappingJob(
        mappingJobExecution,
        sourceSettings,
        sinkSettings,
        terminologyServiceSettings,
        identityServiceSettings,
        Some(timeRange)
      )
      .map(_ => {
        val writer =
          new FileWriter(s"${folderUri.getPath}/${mappingJobExecution.jobId}.txt", true)
        try writer.write(timeRange._2.toString + "\n")
        finally writer.close() // write last sync time to the file
      })
  }

  /**
   * Reads the latest synchronization time point for the job from its last-sync file, returning the
   * time range (lastSyncTime, now) — or (startTime, now) when no sync has happened yet.
   */
  private def getScheduledTimeRange(
      mappingJobId: String,
      folderUri: URI,
      startTime: LocalDateTime
  ): (LocalDateTime, LocalDateTime) = {
    val file = new File(folderUri)
    if (!file.exists || !file.isDirectory) {
      file.mkdirs()
    }
    try {
      val source = Source.fromFile(s"${folderUri.getPath}/$mappingJobId.txt") // read last sync time from file
      val lines = source.getLines()
      val lastLine = lines.foldLeft("") { case (_, line) => line }
      (LocalDateTime.parse(lastLine), LocalDateTime.now()) // (lastSyncTime, currentTime)}
    } catch {
      case _: FileNotFoundException => (startTime, LocalDateTime.now())
    }
  }
}
