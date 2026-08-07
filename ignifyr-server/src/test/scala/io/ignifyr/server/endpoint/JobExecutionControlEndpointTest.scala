package io.ignifyr.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import io.ignifyr.engine.model._
import io.ignifyr.engine.util.FhirMappingJobFormatter.formats
import io.ignifyr.server.BaseEndpointTest
import org.json4s.JArray
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

/**
 * The execution-control half of the job API — status, list executions, stop, deschedule. Starting an
 * execution needs a real source and a FHIR server (that is the long-tier `MappingExecutionEndpointTest`),
 * but the answers for a job that is *not* running are pure registry lookups, and they are what the web UI
 * polls on every job page. They also have to distinguish "no such job" (404) from "job exists, nothing
 * running" (200 with an empty list) — collapsing those two is the mistake this suite guards against.
 */
class JobExecutionControlEndpointTest extends BaseEndpointTest {

  private val job: FhirMappingJob = FhirMappingJob(
    name = Some("execution-control-job"),
    sourceSettings = Map.empty,
    sinkSettings = FileSystemSinkSettings(path = "./out", contentType = SinkContentTypes.CSV),
    mappings = Seq.empty,
    dataProcessingSettings = DataProcessingSettings()
  )

  private def jobUri(jobId: String = job.id): String =
    s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}/$jobId"

  "The job execution control endpoints" should {

    // Note the quotes: the route stringifies the boolean before marshalling, so the body is the JSON
    // string "false", not the JSON literal false. That is the contract the web UI parses.
    "report a job that was never started as not running" in {
      Get(s"${jobUri()}/${JobEndpoint.SEGMENT_STATUS}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "\"false\""
      }
    }

    "report an unknown job as not running rather than failing" in {
      // The status route consults only the running-job registry, so it does not resolve the job at all.
      Get(s"${jobUri("no-such-job")}/${JobEndpoint.SEGMENT_STATUS}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "\"false\""
      }
    }

    "return an empty execution list for a job that has never run" in {
      Get(s"${jobUri()}/${JobEndpoint.SEGMENT_EXECUTIONS}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        JsonMethods.parse(responseAs[String]).asInstanceOf[JArray].arr shouldBe empty
      }
    }

    "return 404 when listing the executions of a job that does not exist" in {
      Get(s"${jobUri("no-such-job")}/${JobEndpoint.SEGMENT_EXECUTIONS}") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    // Stopping every execution of a job is idempotent on purpose: the UI offers it whenever a job page is
    // open, and it must not fail just because nothing happens to be running.
    "accept a request to stop all executions of a job that is not running" in {
      Delete(s"${jobUri()}/${JobEndpoint.SEGMENT_EXECUTIONS}") ~> route ~> check {
        status.isSuccess() shouldBe true
      }
    }

    "return 404 when stopping an execution that is not running" in {
      Delete(
        s"${jobUri()}/${JobEndpoint.SEGMENT_EXECUTIONS}/no-such-execution/${JobEndpoint.SEGMENT_STOP}"
      ) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "return 404 when stopping a mappingTask execution that is not running" in {
      Delete(
        s"${jobUri()}/${JobEndpoint.SEGMENT_EXECUTIONS}/no-such-execution/" +
          s"${JobEndpoint.SEGMENT_MAPPINGS}/no-such-mapping/${JobEndpoint.SEGMENT_STOP}"
      ) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "return 404 when descheduling an execution that is not scheduled" in {
      Delete(
        s"${jobUri()}/${JobEndpoint.SEGMENT_EXECUTIONS}/no-such-execution/${JobEndpoint.SEGMENT_DESCHEDULE}"
      ) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }
  }

  /**
   * Creates the project and the job the execution-control routes are exercised against.
   * */
  override def beforeAll(): Unit = {
    super.beforeAll()
    this.createProject()
    Post(
      s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}",
      HttpEntity(ContentTypes.`application/json`, writePretty(job))
    ) ~> route ~> check {
      status shouldEqual StatusCodes.Created
    }
  }
}
