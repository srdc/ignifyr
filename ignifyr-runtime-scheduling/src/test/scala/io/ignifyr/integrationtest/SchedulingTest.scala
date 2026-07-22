package io.ignifyr.integrationtest

import akka.http.scaladsl.model.StatusCodes
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.api.util.FHIRUtil
import io.onfhir.path.FhirPathUtilFunctionsFactory
import io.onfhir.util.JsonFormatter.formats
import io.ignifyr.{OnFhirTestContainer, IgnifyrTestSpec}
import io.ignifyr.engine.mapping.context.MappingContextLoader
import io.ignifyr.engine.mapping.job.FhirMappingJobManager
import io.ignifyr.engine.model.{FhirMappingJob, FhirMappingJobExecution, FhirRepositorySinkSettings}
import io.ignifyr.engine.util.{FhirMappingJobFormatter, FhirMappingUtility}
import io.ignifyr.runtime.scheduling.Cron4jSchedulerProvider
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.nio.file.Files
import java.sql.{Connection, DriverManager, Statement}
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Using}

/**
 * Re-homed from the engine (the parked `SchedulingTest`) and rewritten onto the extracted
 * [[Cron4jSchedulerProvider]] SPI: the provider now owns and starts the cron4j scheduler internally,
 * so the test only schedules the job and tears it down via `descheduleJobExecution`. Drives a
 * cron-scheduled SQL->FHIR incremental sync end to end (H2 source + onFHIR container), so it needs
 * the SQL connector on its test classpath for the SqlSource reader to be ServiceLoader-discovered.
 */
class SchedulingTest extends AnyFlatSpec with BeforeAndAfterAll with IgnifyrTestSpec with OnFhirTestContainer {

  import io.ignifyr.engine.Execution.actorSystem.dispatcher

  val DATABASE_URL = "jdbc:h2:mem:inputDb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE"

  // The provider owns and starts cron4j; the test schedules and, in teardown, deschedules. Its
  // per-job last-synchronization files are written under an isolated temp directory.
  private val schedulerProvider = new Cron4jSchedulerProvider
  private val ignifyrDbFolderPath: String = Files.createTempDirectory("ignifyr-scheduling-test").toString

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val sql = readFileContent("/sql/scheduling-populate.sql")
    runSQL(sql)
  }

  override protected def afterAll(): Unit = {
    deleteResources()
    val sql = readFileContent("/sql/scheduling-drop.sql")
    runSQL(sql)
    super.afterAll()
  }

  private def readFileContent(fileName: String): String = {
    val source: BufferedSource = Source.fromInputStream(getClass.getResourceAsStream(fileName))
    try source.mkString
    finally source.close()
  }

  private def runSQL(sql: String): Boolean = {
    Using.Manager { use =>
      val con: Connection = use(DriverManager.getConnection(DATABASE_URL))
      val stm: Statement = use(con.createStatement)
      stm.execute(sql)
    } match {
      case Success(value) => value
      case Failure(e) => throw e
    }
  }

  private def deleteResources(): Assertion = {
    var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
    // Delete all patients between p1-p10 and related observation
    (1 to 10).foreach(i => {
      batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p" + i)))
    })
    val f = onFhirClient
      .search("Observation")
      .where("subject", "Patient/" + FhirMappingUtility.getHashedId("Patient", "p4")) flatMap { observationBundle =>
      observationBundle.searchResults.foreach(obs => {
        batchRequest = batchRequest.entry(_.delete("Observation", (obs \ "id").extract[String]))
      })
      batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
        res.httpStatus shouldBe StatusCodes.OK
      }
    }
    Await.result(f, Duration.Inf)
  }

  val fhirSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = onFhirClient.getBaseUrl())

  val testScheduleMappingJobFilePath: String = getClass.getResource("/test-schedule-mappingjob.json").toURI.getPath

  it should "schedule a FhirMappingJob with cron and sink settings restored from a file" in {
    val lMappingJob: FhirMappingJob = FhirMappingJobFormatter.readMappingJobFromFile(testScheduleMappingJobFilePath)

    val fhirMappingJobManager = new FhirMappingJobManager(
      mappingRepository,
      new MappingContextLoader,
      schemaRepository,
      Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory),
      sparkSession
    )
    val mappingJobExecution = FhirMappingJobExecution(mappingTasks = lMappingJob.mappings, job = lMappingJob)
    // The provider validates the cron expression, registers the execution and starts the scheduler.
    schedulerProvider.scheduleMappingJob(
      jobManager = fhirMappingJobManager,
      runningJobRegistry = runningJobRegistry,
      ignifyrDbFolderPath = ignifyrDbFolderPath,
      mappingJobExecution = mappingJobExecution,
      sourceSettings = lMappingJob.sourceSettings,
      sinkSettings =
        lMappingJob.sinkSettings.asInstanceOf[FhirRepositorySinkSettings].copy(fhirRepoUrl = onFhirClient.getBaseUrl()),
      schedulingSettings = lMappingJob.schedulingSettings.get,
      terminologyServiceSettings = None,
      identityServiceSettings = None
    )
    Thread.sleep(61000) // wait for the job to be executed once (cron runs every minute)
    schedulerProvider.descheduleJobExecution(runningJobRegistry, mappingJobExecution.jobId, mappingJobExecution.id)

    val searchTest =
      onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p8")).executeAndReturnResource() flatMap {
        p1Resource =>
          FHIRUtil.extractIdFromResource(p1Resource) shouldBe FhirMappingUtility.getHashedId("Patient", "p8")
          FHIRUtil.extractValue[String](p1Resource, "gender") shouldBe "female"
          FHIRUtil.extractValue[String](p1Resource, "birthDate") shouldBe "2010-01-10"

          onFhirClient.search("Observation").where("code", "9269-2").executeAndReturnBundle() flatMap {
            observationBundle =>
              // the Observation with the code 9269-2 matches our time range, others should not
              observationBundle.searchResults.length shouldBe 1
              (observationBundle.searchResults.head \ "subject" \ "reference").extract[String] shouldBe
                FhirMappingUtility.getHashedReference("Patient", "p4")
              // the Observation with the code 445619006, as an example, does not match our time range
              onFhirClient.search("Observation").where("code", "445619006").executeAndReturnBundle() map {
                emptyObservationBundle =>
                  emptyObservationBundle.searchResults shouldBe empty
              }
          }
      }
    Await.result(searchTest, Duration.Inf)
  }

}
