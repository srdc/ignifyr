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
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.io.File
import java.util.Properties
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * End-to-end Kafka streaming test, simulating the REDCap ingestion path WITHOUT the external
 * `tofhir-redcap` service. In production, `tofhir-redcap` subscribes to REDCap and publishes each
 * record as a JSON message to a Kafka topic; Ignifyr consumes it with an ordinary `KafkaSource`
 * (`asRedCap` was removed as dead config — commit 912d15cb). Here a `KafkaContainer` stands in and
 * we publish REDCap-shaped patient records directly to the topic, exercising the exact Kafka
 * streaming path: `ignifyr-connector-kafka` builds the source dataset, this module's
 * `StreamingJobExecutor` runs the query, and records flow through `patient-mapping` into onFHIR.
 *
 * Integration phase (Docker): MongoDB + srdc/onfhir:r5 (OnFhirTestContainer) + a Kafka container.
 * Uses `rp*` patient ids so it never collides with the folder-watch suite's `p*` ids.
 */
class KafkaStreamingRedcapTest
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with IgnifyrTestSpec
    with OnFhirTestContainer {

  import io.ignifyr.engine.Execution.actorSystem.dispatcher

  private val jobId = "kafka-streaming-redcap-test"
  private val topic = "redcap-patients"

  private val kafka: KafkaContainer =
    new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0")).withReuse(true)

  private var query: Option[StreamingQuery] = None

  private val fhirMappingJobManager = new FhirMappingJobManager(
    mappingRepository,
    new MappingContextLoader,
    schemaRepository,
    Map.empty,
    sparkSession
  )

  // REDCap-shaped records — a JSON object per row keyed by the `Ext-patient` schema fields, exactly
  // what tofhir-redcap would push to the topic.
  private val redcapRecords: Seq[String] = Seq(
    """{"pid":"rp1","gender":"male","birthDate":"1980-01-01","deceasedDateTime":"","homePostalCode":"K01000"}""",
    """{"pid":"rp2","gender":"female","birthDate":"1992-07-15","deceasedDateTime":"","homePostalCode":"K02000"}"""
  )

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    kafka.start()
    org.apache.commons.io.FileUtils.deleteQuietly(new File(IgnifyrConfig.sparkCheckpointDirectory, jobId))
    publishRecords()
  }

  override protected def afterAll(): Unit = {
    query.foreach(q => if (q.isActive) q.stop())
    deleteResources()
    org.apache.commons.io.FileUtils.deleteQuietly(new File(IgnifyrConfig.sparkCheckpointDirectory, jobId))
    if (kafka.isRunning) kafka.stop()
    super.afterAll()
  }

  private def publishRecords(): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafka.getBootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    try redcapRecords.foreach(r => producer.send(new ProducerRecord[String, String](topic, r)).get())
    finally {
      producer.flush()
      producer.close()
    }
  }

  private def deleteResources(): Unit = {
    var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
    Seq("rp1", "rp2").foreach { id =>
      batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", id)))
    }
    Await.result(
      batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute(),
      60.seconds
    )
  }

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
    assert(
      found,
      s"Patient '$hashedId' did not appear in onFHIR within $timeout (Kafka streaming did not consume the topic)"
    )
  }

  // lazy: `kafka.getBootstrapServers` is only valid after `kafka.start()` in beforeAll, so this must
  // not be evaluated at construction time.
  private lazy val streamingJob: FhirMappingJob = FhirMappingJob(
    id = jobId,
    sourceSettings = Map(
      "source" -> KafkaSourceSettings(
        name = "redcap-kafka-source",
        sourceUri = "https://aiccelerate.eu/data-integration-suite/redcap-kafka",
        bootstrapServers = kafka.getBootstrapServers,
        asStream = true
      )
    ),
    sinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = onFhirClient.getBaseUrl()),
    mappings = Seq(
      FhirMappingTask(
        name = "patient-mapping",
        mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
        sourceBinding = Map("source" -> KafkaSource(topicName = topic, options = Map("startingOffsets" -> "earliest")))
      )
    ),
    dataProcessingSettings = DataProcessingSettings(archiveMode = ArchiveModes.OFF, saveErroneousRecords = false)
  )

  it should "consume REDCap-shaped records from Kafka and write FHIR Patients (Kafka streaming)" in {
    val streams: Map[String, Future[StreamingQuery]] = fhirMappingJobManager.startMappingJobStream(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = streamingJob.mappings, job = streamingJob),
      sourceSettings = streamingJob.sourceSettings,
      sinkSettings = streamingJob.sinkSettings
    )
    query = streams.values.headOption.map(f => Await.result(f, 60.seconds))
    query shouldBe defined

    awaitPatient("rp1")
    awaitPatient("rp2")

    val rp2 = Await.result(
      onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "rp2")).executeAndReturnResource(),
      15.seconds
    )
    FHIRUtil.extractValue[String](rp2, "gender") shouldBe "female"
  }
}
