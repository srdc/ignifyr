package io.ignifyr.test.engine.execution

import io.ignifyr.engine.IgnifyrEngine
import io.ignifyr.engine.execution.MappingJobLauncher
import io.ignifyr.engine.model._
import org.mockito.MockitoSugar._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Skipping the write is supported only for batch executions: a streaming execution would still advance its
 * checkpoints and a scheduled one its last-sync time, so a later real run would silently skip the data the
 * skipped run consumed. The launcher, the single dispatch point for the CLI and the server, refuses both
 * before anything starts.
 */
class MappingJobLauncherSkipWriteTest extends AnyFlatSpec with Matchers {

  private val launcher = new MappingJobLauncher(mock[IgnifyrEngine])

  private val sinkSettings = FileSystemSinkSettings("./out", SinkContentTypes.NDJSON)

  private def job(asStream: Boolean = false, scheduling: Option[BaseSchedulingSettings] = None): FhirMappingJob =
    FhirMappingJob(
      id = "job-1",
      sourceSettings = Map("_" -> FileSystemSourceSettings("test", "test", "data", asStream = asStream)),
      sinkSettings = sinkSettings,
      mappings = Seq.empty,
      schedulingSettings = scheduling
    )

  private def skippedExecution(mappingJob: FhirMappingJob): FhirMappingJobExecution =
    FhirMappingJobExecution(job = mappingJob, skipWrite = true)

  "launch" should "refuse to skip the write of a streaming job" in {
    val streamingJob = job(asStream = true)
    an[IllegalArgumentException] should be thrownBy launcher.launch(streamingJob, skippedExecution(streamingJob))
  }

  it should "refuse to skip the write of a scheduled job" in {
    val scheduledJob = job(scheduling = Some(SchedulingSettings("* * * * *")))
    an[IllegalArgumentException] should be thrownBy launcher.launch(scheduledJob, skippedExecution(scheduledJob))
  }
}
