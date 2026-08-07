package io.ignifyr.integrationtest

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.Logger
import io.onfhir.api.Resource
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.api.util.FHIRUtil
import io.onfhir.path.FhirPathUtilFunctionsFactory
import io.onfhir.util.JsonFormatter._
import io.ignifyr.engine.mapping.context.MappingContextLoader
import io.ignifyr.{OnFhirTestContainer, IgnifyrTestSpec}
import io.ignifyr.engine.mapping.job.FhirMappingJobManager
import io.ignifyr.engine.model._
import io.ignifyr.engine.util.{FhirMappingJobFormatter, FhirMappingUtility}
import org.json4s.JsonAST.JObject
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec

import java.sql.{Connection, DriverManager, Statement}
import scala.concurrent.Future
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Using}

class SqlSourceTest extends AsyncFlatSpec with BeforeAndAfterAll with IgnifyrTestSpec with OnFhirTestContainer {

  val logger: Logger = Logger(this.getClass)

  val DATABASE_URL = "jdbc:h2:mem:inputDb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val sql = readFileContent("/sql/sql-source-populate.sql")
    runSQL(sql)
  }

  override protected def afterAll(): Unit = {
    val sql = readFileContent("/sql/sql-source-drop.sql")
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

  val testSqlMappingJobFilePath: String = getClass.getResource("/test-sql-mappingjob.json").toURI.getPath

  val sqlSourceSettings =
    Map(
      "source" ->
        SqlSourceSettings(
          name = "test-db-source",
          sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data",
          databaseUrl = DATABASE_URL,
          username = "",
          password = ""
        )
    )

  val fhirMappingJobManager = new FhirMappingJobManager(
    mappingRepository,
    contextLoader,
    schemaRepository,
    Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory),
    sparkSession
  )

  val fhirSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = onFhirClient.getBaseUrl())

  // sql tablename mappings tasks
  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    name = "patient-sql-mapping",
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-sql-mapping",
    sourceBinding = Map("source" -> SqlSource(tableName = Some("patients")))
  )

  val otherObsMappingTask: FhirMappingTask = FhirMappingTask(
    name = "other-observation-sql-mapping",
    mappingRef = "https://aiccelerate.eu/fhir/mappings/other-observation-sql-mapping",
    sourceBinding = Map("source" -> SqlSource(tableName = Some("otherobservations")))
  )

  // sql query mappings tasks
  val careSiteMappingTask: FhirMappingTask = FhirMappingTask(
    name = "care-site-sql-mapping",
    mappingRef = "https://aiccelerate.eu/fhir/mappings/care-site-sql-mapping",
    sourceBinding = Map(
      "source" -> SqlSource(
        query = Some(
          "select cs.care_site_id, cs.care_site_name, c.concept_code, c.vocabulary_id, c.concept_name, l.address_1, l.address_2, l.city, l.state, l.zip " +
            "from care_site cs, location l, concept c " +
            "where cs.location_id = l.location_id and cs.place_of_service_concept_id = c.concept_id"
        )
      )
    )
  )

  val locationMappingTask: FhirMappingTask = FhirMappingTask(
    name = "location-sql-mapping",
    mappingRef = "https://aiccelerate.eu/fhir/mappings/location-sql-mapping",
    sourceBinding = Map("source" -> SqlSource(query = Some("select * from location")))
  )

  val procedureOccurrenceMappingTask: FhirMappingTask = FhirMappingTask(
    name = "procedure-occurrence-sql-mapping",
    mappingRef = "https://aiccelerate.eu/fhir/mappings/procedure-occurrence-sql-mapping",
    sourceBinding = Map(
      "source" -> SqlSource(
        query = Some(
          "select po.procedure_occurrence_id, po.visit_occurrence_id, po.person_id, c.concept_code, c.vocabulary_id, c.concept_name, " +
            "po.procedure_date, po.procedure_datetime, po.provider_id " +
            "from procedure_occurrence po left join concept c on po.procedure_concept_id = c.concept_id"
        )
      )
    )
  )

  val fhirMappingJob: FhirMappingJob = FhirMappingJob(
    name = Some("test-sql-mappingjob"),
    mappings = Seq.empty,
    sourceSettings = sqlSourceSettings,
    sinkSettings = fhirSinkSettings,
    dataProcessingSettings = DataProcessingSettings()
  )

  "Patient mapping" should "should read data from SQL source and map it" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTask), job = fhirMappingJob),
      mappingJobSourceSettings = sqlSourceSettings
    ) map { mappingResults =>
      val results = mappingResults.map(r => {
        r.mappedFhirResource.get.mappedResource should not be None
        val resource = r.mappedFhirResource.get.mappedResource.get.parseJson
        resource shouldBe a[Resource]
        resource
      })
      results.size shouldBe 10
      val patient1 = results.head
      FHIRUtil.extractResourceType(patient1) shouldBe "Patient"
      FHIRUtil.extractIdFromResource(patient1) shouldBe FhirMappingUtility.getHashedId("Patient", "p1")
      FHIRUtil.extractValue[String](patient1, "gender") shouldBe "male"
      FHIRUtil.extractValue[String](patient1, "birthDate") shouldBe "2000-05-10"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    // Send it to our fhir repo if they are also validated
    fhirMappingJobManager
      .executeMappingJob(
        mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(patientMappingTask), job = fhirMappingJob),
        sourceSettings = sqlSourceSettings,
        sinkSettings = fhirSinkSettings
      )
      .flatMap(_ => {
        // Delete patients
        var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
        (1 to 10).foreach { i =>
          batchRequest =
            batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p" + i.toString)))
        }
        batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
          res.httpStatus shouldBe StatusCodes.OK
        }
      })
  }

  "Other observations mapping" should "should read data from SQL source and map it" in {
    fhirMappingJobManager.executeMappingTaskAndReturn(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(otherObsMappingTask), job = fhirMappingJob),
      mappingJobSourceSettings = sqlSourceSettings
    ) map { mappingResults =>
      val results = mappingResults.map(r => {
        r.mappedFhirResource.get.mappedResource should not be None
        val resource = r.mappedFhirResource.get.mappedResource.get.parseJson
        resource shouldBe a[Resource]
        resource
      })
      results.size shouldBe 14
      val observation = results.head
      FHIRUtil.extractResourceType(observation) shouldBe "Observation"
      (observation \ "encounter" \ "reference")
        .extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "e1")
      (observation \ "code" \ "coding" \ "code").extract[Seq[String]].head shouldBe "9110-8"
      (observation \ "valueQuantity" \ "value").extract[Int] shouldBe 450
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    fhirMappingJobManager
      .executeMappingJob(
        mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(otherObsMappingTask), job = fhirMappingJob),
        sourceSettings = sqlSourceSettings,
        sinkSettings = fhirSinkSettings
      )
      .flatMap(_ => {
        // Delete all observations
        var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
        val obsSearchFutures = (1 to 10).map(i => {
          onFhirClient
            .search("Observation")
            .where("subject", "Patient/" + FhirMappingUtility.getHashedId("Patient", "p" + i))
            .executeAndReturnBundle()
        })
        // This mapping emits MedicationAdministrations alongside the Observations. Those used to be
        // rejected by the R5 server while the test still passed, because it only checked that the
        // cleanup delete answered 200 — so assert both kinds actually landed before deleting them.
        val medSearchFutures = (1 to 10).map(i =>
          onFhirClient
            .search("MedicationAdministration")
            .where("subject", "Patient/" + FhirMappingUtility.getHashedId("Patient", "p" + i))
            .executeAndReturnBundle()
        )
        Future.sequence(obsSearchFutures) flatMap { obsBundleList =>
          Future.sequence(medSearchFutures) flatMap { medBundleList =>
            obsBundleList.flatMap(_.searchResults) should not be empty
            medBundleList.flatMap(_.searchResults) should not be empty

            obsBundleList.foreach(observationBundle => {
              observationBundle.searchResults
                .foreach(obs =>
                  batchRequest = batchRequest.entry(_.delete("Observation", (obs \ "id").extract[String]))
                )
            })
            medBundleList.foreach(medicationBundle => {
              medicationBundle.searchResults.foreach(med =>
                batchRequest = batchRequest.entry(_.delete("MedicationAdministration", (med \ "id").extract[String]))
              )
            })
            batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
              res.httpStatus shouldBe StatusCodes.OK
            }
          }
        }
      })
  }

  "Care site mapping" should "should read data from SQL source and map it" in {
    val fhirMappingJobManager =
      new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map.empty, sparkSession)
    fhirMappingJobManager.executeMappingTaskAndReturn(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(careSiteMappingTask), job = fhirMappingJob),
      mappingJobSourceSettings = sqlSourceSettings
    ) map { mappingResults =>
      val results = mappingResults.map(r => {
        r.mappedFhirResource.get.mappedResource should not be None
        val resource = r.mappedFhirResource.get.mappedResource.get.parseJson
        resource shouldBe a[Resource]
        resource
      })
      results.size shouldBe 2
      val organization1 = results.head
      FHIRUtil.extractResourceType(organization1) shouldBe "Organization"
      (organization1 \ "name").extract[String] shouldBe "Example care site name"
      (((organization1 \ "type").extract[Seq[JObject]].head \ "coding").extract[Seq[JObject]].head \ "code")
        .extract[String] shouldBe "21"
      // R5 moved Organization.address under contact (ExtendedContactDetail).
      (((organization1 \ "contact").extract[Seq[JObject]].head \ "address") \ "state").extract[String] shouldBe "MO"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    // Send it to our fhir repo if they are also validated
    fhirMappingJobManager
      .executeMappingJob(
        mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(careSiteMappingTask), job = fhirMappingJob),
        sourceSettings = sqlSourceSettings,
        sinkSettings = fhirSinkSettings
      )
      .flatMap(_ => {
        // Read the written resources back before deleting them. Without this the test asserted only that
        // the cleanup delete answered 200, which it does whether or not anything was ever written — and
        // for a while nothing was, because the mapping emitted an R4-shaped Organization the R5 server
        // rejected.
        Future
          .sequence((1 to 2).map { i =>
            onFhirClient
              .read("Organization", FhirMappingUtility.getHashedId("Organization", i.toString))
              .executeAndReturnResource()
          })
          .flatMap { organizations =>
            organizations.size shouldBe 2
            organizations.foreach(FHIRUtil.extractResourceType(_) shouldBe "Organization")

            // Delete care sites
            var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
            (1 to 2).foreach { i =>
              batchRequest =
                batchRequest.entry(_.delete("Organization", FhirMappingUtility.getHashedId("Organization", i.toString)))
            }
            batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
              res.httpStatus shouldBe StatusCodes.OK
            }
          }
      })
  }

  "Location mapping" should "should read data from SQL source and map it" in {
    val fhirMappingJobManager =
      new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map.empty, sparkSession)
    fhirMappingJobManager.executeMappingTaskAndReturn(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(locationMappingTask), job = fhirMappingJob),
      mappingJobSourceSettings = sqlSourceSettings
    ) map { mappingResults =>
      val results = mappingResults.map(r => {
        r.mappedFhirResource.get.mappedResource should not be None
        val resource = r.mappedFhirResource.get.mappedResource.get.parseJson
        resource shouldBe a[Resource]
        resource
      })
      results.size shouldBe 5
      val location = results.head
      FHIRUtil.extractResourceType(location) shouldBe "Location"
      ((location \ "address").extract[JObject] \ "line").extract[Seq[String]].head shouldBe "19 Farragut"
      ((location \ "address").extract[JObject] \ "state").extract[String] shouldBe "MO"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    fhirMappingJobManager
      .executeMappingJob(
        mappingJobExecution = FhirMappingJobExecution(mappingTasks = Seq(locationMappingTask), job = fhirMappingJob),
        sourceSettings = sqlSourceSettings,
        sinkSettings = fhirSinkSettings
      )
      .flatMap(_ => {
        // Delete locations
        var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
        (1 to 5).foreach { i =>
          batchRequest =
            batchRequest.entry(_.delete("Location", FhirMappingUtility.getHashedId("Location", i.toString)))
        }
        batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
          res.httpStatus shouldBe StatusCodes.OK
        }
      })
  }

  "Procedure occurrence mapping" should "should read data from SQL source and map it" in {
    val fhirMappingJobManager =
      new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map.empty, sparkSession)
    fhirMappingJobManager.executeMappingTaskAndReturn(
      mappingJobExecution =
        FhirMappingJobExecution(mappingTasks = Seq(procedureOccurrenceMappingTask), job = fhirMappingJob),
      mappingJobSourceSettings = sqlSourceSettings
    ) map { mappingResults =>
      val results = mappingResults.map(r => {
        r.mappedFhirResource.get.mappedResource should not be None
        val resource = r.mappedFhirResource.get.mappedResource.get.parseJson
        resource shouldBe a[Resource]
        resource
      })
      results.size shouldBe 5
      val procedureOccurrence = results.head
      FHIRUtil.extractResourceType(procedureOccurrence) shouldBe "Procedure"
      (procedureOccurrence \ "subject" \ "reference")
        .extract[String] shouldBe FhirMappingUtility.getHashedReference("Patient", "906440")
      (procedureOccurrence \ "encounter" \ "reference")
        .extract[String] shouldBe FhirMappingUtility.getHashedReference("Encounter", "43483680")
      ((procedureOccurrence \ "performer").extract[Seq[JObject]].head \ "actor" \ "reference")
        .extract[String] shouldBe FhirMappingUtility.getHashedReference("Practitioner", "48878")
      // R5 renamed Procedure.performed[x] to occurrence[x] -- with two r's, unlike
      // MedicationAdministration.occurence[x], which the spec spells with one.
      (procedureOccurrence \ "occurrenceDateTime").extract[String] shouldBe "2010-04-25"
    }
  }

  it should "map test data and write it to FHIR repo successfully" in {
    fhirMappingJobManager
      .executeMappingJob(
        mappingJobExecution =
          FhirMappingJobExecution(mappingTasks = Seq(procedureOccurrenceMappingTask), job = fhirMappingJob),
        sourceSettings = sqlSourceSettings,
        sinkSettings = fhirSinkSettings
      )
      .flatMap(_ => {
        // Delete procedures
        var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
        (1 to 5).foreach { i =>
          batchRequest =
            batchRequest.entry(_.delete("Procedure", FhirMappingUtility.getHashedId("Procedure", i.toString)))
        }
        batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
          res.httpStatus shouldBe StatusCodes.OK
        }
      })
  }

  /*
   * The orchestration half of the batching strategy: one execution per entry of `batchParameterSets`,
   * run sequentially, with the entry's values substituted into the task's `preprocessSql`. The
   * substitution itself is unit-tested in the engine (`FhirMappingTaskTest`); what needs a real source
   * and sink is that *every* set runs and their outputs accumulate — a fold that kept only the last
   * result, or stopped after the first, would still produce a green job and silently drop data.
   *
   * The `patients` fixture holds five male and five female rows, so batching by gender writes all ten
   * only if both batches executed.
   */
  "Batched patient mapping" should "run the mapping once per batch parameter set" in {
    val batchedPatientMappingTask: FhirMappingTask = FhirMappingTask(
      name = "patient-sql-mapping",
      mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-sql-mapping",
      sourceBinding = Map(
        "source" -> SqlSource(
          tableName = Some("patients"),
          preprocessSql = Some("SELECT * FROM source WHERE gender = '$gender'")
        )
      ),
      batchingStrategy = Some(BatchingStrategy(Seq(Map("gender" -> "male"), Map("gender" -> "female"))))
    )

    fhirMappingJobManager
      .executeMappingJob(
        mappingJobExecution =
          FhirMappingJobExecution(mappingTasks = Seq(batchedPatientMappingTask), job = fhirMappingJob),
        sourceSettings = sqlSourceSettings,
        sinkSettings = fhirSinkSettings
      )
      .flatMap { _ =>
        // p1 can only come from the "male" batch and p8 only from the "female" one.
        val fromFirstBatch =
          onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p1")).executeAndReturnResource()
        val fromLastBatch =
          onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p8")).executeAndReturnResource()

        for {
          male <- fromFirstBatch
          female <- fromLastBatch
          cleanup <- {
            FHIRUtil.extractValue[String](male, "gender") shouldBe "male"
            FHIRUtil.extractValue[String](female, "gender") shouldBe "female"
            var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
            (1 to 10).foreach { i =>
              batchRequest =
                batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p" + i.toString)))
            }
            batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute()
          }
        } yield cleanup.httpStatus shouldBe StatusCodes.OK
      }
  }

  it should "execute the FhirMappingJob with SQL source and sink settings restored from a file" in {
    val lMappingJob = FhirMappingJobFormatter.readMappingJobFromFile(testSqlMappingJobFilePath)

    val fhirMappingJobManager =
      new FhirMappingJobManager(mappingRepository, new MappingContextLoader, schemaRepository, Map.empty, sparkSession)
    fhirMappingJobManager.executeMappingJob(
      mappingJobExecution = FhirMappingJobExecution(mappingTasks = lMappingJob.mappings, job = lMappingJob),
      sourceSettings = lMappingJob.sourceSettings,
      sinkSettings =
        lMappingJob.sinkSettings.asInstanceOf[FhirRepositorySinkSettings].copy(fhirRepoUrl = onFhirClient.getBaseUrl())
    ) flatMap { unit =>
      // Delete written resources
      var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
      (1 to 10).foreach { i =>
        batchRequest =
          batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p" + i.toString)))
      }
      (1 to 2).foreach { i =>
        batchRequest =
          batchRequest.entry(_.delete("Organization", FhirMappingUtility.getHashedId("Organization", i.toString)))
      }
      (1 to 5).foreach { i =>
        batchRequest = batchRequest.entry(_.delete("Location", FhirMappingUtility.getHashedId("Location", i.toString)))
      }
      batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
        res.httpStatus shouldBe StatusCodes.OK
      }
    }
  }

}
