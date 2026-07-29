package io.ignifyr.integrationtest

import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.api.util.FHIRUtil
import io.onfhir.util.JsonFormatter.formats
import io.ignifyr.{IgnifyrTestSpec, OnFhirTestContainer}
import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.mapping.context.MappingContextLoader
import io.ignifyr.engine.mapping.job.FhirMappingJobManager
import io.ignifyr.engine.model._
import io.ignifyr.engine.util.FhirMappingUtility
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * End-to-end folder-watch streaming test — fills the gap left by the mechanics-only
 * `StreamingSinkHandlerTest` (rate source) and the fabricated `FileStreamInputArchiverTest`.
 *
 * It exercises the real capability seam: the community engine builds the file-stream source dataset
 * (via `ignifyr-connector-file`, on the test classpath) and this module's `StreamingJobExecutor`
 * (discovered through the `IgnifyrExtension` ServiceLoader SPI) starts and writes the Spark
 * structured-streaming query. A CSV dropped into the watched directory after the query starts must
 * flow through the `patient-mapping` (testkit fixture, schema `Ext-patient`) and land as FHIR
 * Patient resources in the onFHIR/repofyr container.
 *
 * Runs in the integration phase (needs Docker: MongoDB + srdc/onfhir:r5 via `OnFhirTestContainer`).
 * Archiving of consumed inputs is covered separately by `FileStreamInputArchiverTest`; here
 * `archiveMode = off` so the streaming archiver timer (started only by `IgnifyrEngine`) is not
 * required.
 */
class StreamingFolderWatchTest extends AnyFlatSpec with BeforeAndAfterAll with IgnifyrTestSpec with OnFhirTestContainer {

  import io.ignifyr.engine.Execution.actorSystem.dispatcher

  // A unique job id keeps this job's Spark checkpoint dir (`<checkpoint>/<jobId>/<hash(task)>`)
  // isolated from any other suite so streaming offsets never collide.
  private val jobId = "streaming-folder-watch-test"

  // The directory Spark's file-stream source polls: <dataFolderPath>/<sourceBinding.path>.
  private val watchRoot: Path = Files.createTempDirectory("ignifyr-folder-watch-")
  private val watchedDir: Path = watchRoot.resolve("patients")

  private var queries: Seq[StreamingQuery] = Seq.empty

  private val fhirMappingJobManager = new FhirMappingJobManager(
    mappingRepository,
    new MappingContextLoader,
    schemaRepository,
    Map.empty,
    sparkSession
  )

  private val patientMappingTask: FhirMappingTask = FhirMappingTask(
    name = "patient-mapping",
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceBinding = Map("source" -> FileSystemSource(path = "patients", contentType = SourceContentTypes.CSV))
  )

  private val streamingJob: FhirMappingJob = FhirMappingJob(
    id = jobId,
    sourceSettings = Map(
      "source" -> FileSystemSourceSettings(
        name = "folder-watch-source",
        sourceUri = "https://aiccelerate.eu/data-integration-suite/streaming-test-data",
        dataFolderPath = watchRoot.toAbsolutePath.toString,
        asStream = true
      )
    ),
    sinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = onFhirClient.getBaseUrl()),
    mappings = Seq(patientMappingTask),
    dataProcessingSettings = DataProcessingSettings(archiveMode = ArchiveModes.OFF, saveErroneousRecords = false)
  )

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Files.createDirectories(watchedDir)
    // Fresh checkpoints so the stream reprocesses from scratch on each run.
    org.apache.commons.io.FileUtils.deleteQuietly(new File(IgnifyrConfig.sparkCheckpointDirectory, jobId))
  }

  override protected def afterAll(): Unit = {
    queries.foreach(q => if (q.isActive) q.stop())
    deleteResources()
    org.apache.commons.io.FileUtils.deleteQuietly(watchRoot.toFile)
    org.apache.commons.io.FileUtils.deleteQuietly(new File(IgnifyrConfig.sparkCheckpointDirectory, jobId))
    super.afterAll()
  }

  private def deleteResources(): Unit = {
    var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
    (1 to 10).foreach { i =>
      batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p" + i)))
    }
    Await.result(
      batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute(),
      60.seconds
    )
  }

  /** Copy the CSV fixture into the watched directory so Spark's file-stream source picks it up. */
  private def dropPatientsCsv(): Unit = {
    val in = getClass.getResourceAsStream("/streaming-folder-watch/patients.csv")
    try Files.copy(in, watchedDir.resolve("patients.csv"), StandardCopyOption.REPLACE_EXISTING)
    finally in.close()
  }

  /** Poll onFHIR until the given hashed Patient id is readable, or fail after the timeout. */
  private def awaitPatient(rawId: String, timeout: FiniteDuration = 90.seconds): Unit = {
    val hashedId = FhirMappingUtility.getHashedId("Patient", rawId)
    val deadline = timeout.fromNow
    var found = false
    while (!found && deadline.hasTimeLeft()) {
      val attempt = onFhirClient.read("Patient", hashedId).executeAndReturnResource().map { resource =>
        FHIRUtil.extractIdFromResource(resource) == hashedId
      }
      found =
        try Await.result(attempt, 10.seconds)
        catch { case _: Throwable => false }
      if (!found) Thread.sleep(2000)
    }
    assert(found, s"Patient '$hashedId' did not appear in onFHIR within $timeout (streaming folder-watch did not process the dropped file)")
  }

  it should "process a CSV dropped into the watched folder and write FHIR Patients (folder-watch streaming)" in {
    val streams: Map[String, Future[StreamingQuery]] = fhirMappingJobManager.startMappingJobStream(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = streamingJob.mappings, job = streamingJob),
      sourceSettings = streamingJob.sourceSettings,
      sinkSettings = streamingJob.sinkSettings
    )
    queries = streams.values.map(f => Await.result(f, 60.seconds)).toSeq
    queries should not be empty

    // Drop the input AFTER the query is live so the file-stream source detects it as a new file.
    dropPatientsCsv()

    // Assert a representative slice of the batch landed (p1 male / p8 female per the fixture).
    awaitPatient("p1")
    awaitPatient("p8")

    val p8 = Await.result(
      onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p8")).executeAndReturnResource(),
      15.seconds
    )
    FHIRUtil.extractValue[String](p8, "gender") shouldBe "female"
  }
}
