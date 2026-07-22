package io.ignifyr.engine.execution

import com.typesafe.scalalogging.Logger
import io.ignifyr.engine.IgnifyrEngine
import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.mapping.job.FhirMappingJobManager
import io.ignifyr.engine.model.{FhirMappingJob, FhirMappingJobExecution}
import io.ignifyr.engine.spi.{ExtensionRegistry, MissingCapabilityException}
import org.apache.commons.io.FileUtils

import java.io.File
import scala.concurrent.{ExecutionContext, Future}

/**
 * How a mapping job execution was launched, with the handle(s) a caller may wait on.
 */
sealed trait MappingJobLaunch

object MappingJobLaunch {

  /** A batch execution was submitted; `completion` completes when all mapping tasks have run. */
  final case class Batch(completion: Future[Unit]) extends MappingJobLaunch

  /**
   * A streaming execution was submitted; each future completes once the corresponding streaming
   * query has been initialized and registered (the queries themselves then run until stopped).
   */
  final case class Streaming(queryRegistrations: Seq[Future[Unit]]) extends MappingJobLaunch

  /** The job was handed to the installed scheduling provider; executions fire per the cron schedule. */
  case object Scheduled extends MappingJobLaunch
}

/**
 * The single home of the batch / streaming / scheduled dispatch for mapping job executions. Both
 * the CLI (`CommandLineInterface.runJob`) and the server (`ExecutionService.runJob`) launch jobs
 * through here, so the edition story stays "add jars": a job with streaming sources or
 * schedulingSettings runs unmodified once the corresponding runtime module is on the classpath,
 * and fails with a clear [[MissingCapabilityException]] otherwise.
 *
 * Every launch is registered with the engine's [[RunningJobRegistry]], giving all callers uniform
 * execution tracking and post-batch archiving.
 *
 * A job whose sources stream (`asStream`) launches as streaming; otherwise `schedulingSettings`
 * selects scheduled execution; otherwise it is a plain batch run.
 */
class MappingJobLauncher(ignifyrEngine: IgnifyrEngine)(implicit ec: ExecutionContext) {

  private val logger: Logger = Logger(this.getClass)

  /** The job manager executing the launched jobs, exposed for ad-hoc task runs (e.g. mapping tests). */
  val jobManager: FhirMappingJobManager = new FhirMappingJobManager(
    ignifyrEngine.mappingRepo,
    ignifyrEngine.contextLoader,
    ignifyrEngine.schemaLoader,
    ignifyrEngine.functionLibraries,
    ignifyrEngine.sparkSession
  )

  /**
   * Launch an execution of the given mapping job.
   *
   * @param mappingJob           The mapping job definition (source/sink/scheduling/service settings).
   * @param mappingJobExecution  The execution to launch; carries the mapping tasks to run.
   * @param ignifyrDbFolderPath  Ignifyr database folder (scheduling providers keep last-sync state there).
   * @param clearCheckpoints     For streaming jobs, reset archiving offsets and delete the Spark
   *                             checkpoint directories so the streams start from scratch.
   */
  def launch(
      mappingJob: FhirMappingJob,
      mappingJobExecution: FhirMappingJobExecution,
      ignifyrDbFolderPath: String = IgnifyrConfig.engineConfig.ignifyrDbFolderPath,
      clearCheckpoints: Boolean = false
  ): MappingJobLaunch = {
    if (mappingJob.sourceSettings.exists(_._2.asStream)) {
      if (clearCheckpoints) clearCheckpointDirectories(mappingJobExecution)
      val queryRegistrations = jobManager
        .startMappingJobStream(
          mappingJobExecution,
          sourceSettings = mappingJob.sourceSettings,
          sinkSettings = mappingJob.sinkSettings,
          terminologyServiceSettings = mappingJob.terminologyServiceSettings,
          identityServiceSettings = mappingJob.getIdentityServiceSettings()
        )
        .map(sq => ignifyrEngine.runningJobRegistry.registerStreamingQuery(mappingJobExecution, sq._1, sq._2))
        .toSeq
      MappingJobLaunch.Streaming(queryRegistrations)
    } else if (mappingJob.schedulingSettings.nonEmpty) {
      // Scheduled execution is an installable capability (enterprise ignifyr-runtime-scheduling).
      // Absent a provider, a job with schedulingSettings fails with a clear message.
      val scheduler = ExtensionRegistry.scheduler.getOrElse(
        throw MissingCapabilityException(
          "This job has schedulingSettings. Scheduled execution requires the " +
            "'ignifyr-runtime-scheduling' module (com.pontegra.ignifyr:ignifyr-runtime-scheduling)."
        )
      )
      // scheduleMappingJob validates the cron, registers the execution, and starts the scheduler.
      scheduler.scheduleMappingJob(
        jobManager = jobManager,
        runningJobRegistry = ignifyrEngine.runningJobRegistry,
        ignifyrDbFolderPath = ignifyrDbFolderPath,
        mappingJobExecution = mappingJobExecution,
        sourceSettings = mappingJob.sourceSettings,
        sinkSettings = mappingJob.sinkSettings,
        schedulingSettings = mappingJob.schedulingSettings.get,
        terminologyServiceSettings = mappingJob.terminologyServiceSettings,
        identityServiceSettings = mappingJob.getIdentityServiceSettings()
      )
      MappingJobLaunch.Scheduled
    } else {
      val completion: Future[Unit] = jobManager.executeMappingJob(
        mappingJobExecution = mappingJobExecution,
        sourceSettings = mappingJob.sourceSettings,
        sinkSettings = mappingJob.sinkSettings,
        terminologyServiceSettings = mappingJob.terminologyServiceSettings,
        identityServiceSettings = mappingJob.getIdentityServiceSettings()
      )
      ignifyrEngine.runningJobRegistry.registerBatchJob(
        mappingJobExecution,
        Some(completion),
        s"Spark job for job: ${mappingJobExecution.jobId} mappingTaskNames: " +
          mappingJobExecution.mappingTasks.map(_.name).mkString(" ")
      )
      MappingJobLaunch.Batch(completion)
    }
  }

  /** Reset archiving offsets and delete each mapping task's Spark checkpoint directory. */
  private def clearCheckpointDirectories(mappingJobExecution: FhirMappingJobExecution): Unit =
    mappingJobExecution.mappingTasks.foreach { mappingTask =>
      // Reset the archiving offset so that the archiving starts from scratch
      ignifyrEngine.fileStreamInputArchiver.resetOffset(mappingJobExecution, mappingTask.name)

      val checkpointDirectory: File = new File(mappingJobExecution.getCheckpointDirectory(mappingTask.name))
      FileUtils.deleteDirectory(checkpointDirectory)
      logger.debug(
        s"Deleted checkpoint directory for jobId: ${mappingJobExecution.jobId}, executionId: ${mappingJobExecution.id}, mappingTaskName: ${mappingTask.name}, path: ${checkpointDirectory.getAbsolutePath}"
      )
    }
}
