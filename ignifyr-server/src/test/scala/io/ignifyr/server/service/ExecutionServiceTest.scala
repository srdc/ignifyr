package io.ignifyr.server.service

import com.typesafe.config.ConfigFactory
import io.ignifyr.engine.config.{IgnifyrConfig, IgnifyrEngineConfig}
import io.ignifyr.engine.data.write.FileSystemWriter.SinkContentTypes
import io.ignifyr.engine.model._
import io.ignifyr.engine.util.FileUtils
import io.ignifyr.server.model.ExecuteJobTask
import io.ignifyr.server.repository.job.JobFolderRepository
import io.ignifyr.server.repository.mapping.ProjectMappingFolderRepository
import io.ignifyr.server.repository.schema.SchemaFolderRepository
import org.apache.commons.io
import org.apache.spark.sql.types.StructType
import org.mockito.MockitoSugar._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Paths}
import scala.concurrent.Future

class ExecutionServiceTest extends AsyncWordSpec with Matchers with BeforeAndAfterAll {

  // Name of the folder to keep test data to run file mapping job
  private val testDataFolder: String = "test-data"
  // Ignifyr engine config
  val ignifyrEngineConfig: IgnifyrEngineConfig = new IgnifyrEngineConfig(ConfigFactory.load().getConfig("ignifyr"))
  // FhirMappingJob definition with streaming file source
  val testJob: FhirMappingJob = FhirMappingJob(
    name = Some("testJob"),
    sourceSettings = Map(
      "test" -> FileSystemSourceSettings(
        name = "test-source",
        sourceUri = "test-source",
        dataFolderPath = testDataFolder,
        asStream = true
      )
    ),
    sinkSettings = FileSystemSinkSettings(path = "http://example.com/fhir", contentType = SinkContentTypes.CSV),
    mappings = Seq.apply(
      FhirMappingTask(
        name = "patient-mapping",
        mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
        sourceBinding = Map("source" -> FileSystemSource(path = ".", contentType = SourceContentTypes.CSV))
      )
    ),
    dataProcessingSettings = DataProcessingSettings(archiveMode = ArchiveModes.OFF)
  )
  // Job execution task to clear checkpoint folder
  val testExecuteJobTask: ExecuteJobTask = ExecuteJobTask(clearCheckpoints = true, Option.empty)
  // Mock repositories
  val mappingRepository: ProjectMappingFolderRepository = getMockMappingRepository
  val schemaRepository: SchemaFolderRepository = getMockSchemaRepository
  val mappingJobRepository: JobFolderRepository = getMockMappingJobRepository
  // the execution service instance for the test
  val executionService: ExecutionService =
    new ExecutionService(mappingJobRepository, mappingRepository, schemaRepository)

  "The Execution Service" should {
    "should clear checkpoint directory" in {
      // create an example file for the mapping job in the corresponding checkpoint directory
      val path: String = Paths
        .get(IgnifyrConfig.sparkCheckpointDirectory, testJob.id, testJob.mappings.head.name.hashCode.toString)
        .toString
      val testFile: File = new File(s"$path/test.txt")
      io.FileUtils.createParentDirectories(testFile)
      val fileOutputStream = new FileOutputStream(testFile)
      fileOutputStream.write("test".getBytes("UTF-8"))
      fileOutputStream.close()

      // check whether the file is written to the directory
      val testDirectory: File = new File(path)
      io.FileUtils.sizeOfDirectory(testDirectory) shouldBe >(0L)

      // run the job and expect to clear the created directory
      executionService
        .runJob("testProject", "testJob", Option.empty, Some(testExecuteJobTask))
        .map(_ =>
          // Check whether the directory is deleted after the job has completed
          Files.exists(testDirectory.toPath) shouldBe false
        )
    }
  }

  /**
   * Create the test folders.
   */
  override def beforeAll(): Unit = {
    // Deleting folders to start with a clean environment
    cleanFolders()
    // Create the test data folder to run mapping job
    FileUtils.getPath(testDataFolder).toFile.mkdirs()
  }

  /**
   * Deletes the test folders after all test cases are completed.
   * */
  override def afterAll(): Unit = {
    cleanFolders()
  }

  /**
   * Deletes the context path.
   * */
  private def cleanFolders(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath("").toFile)
  }

  private def getMockMappingJobRepository: JobFolderRepository = {
    val mockMappingJobRepository: JobFolderRepository = mock[JobFolderRepository]
    when(mockMappingJobRepository.getJob("testProject", "testJob")).thenReturn(Future.apply(Some(testJob)))
  }

  private def getMockMappingRepository: ProjectMappingFolderRepository = {
    val mockMappingRepository: ProjectMappingFolderRepository = mock[ProjectMappingFolderRepository]
    when(mockMappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/patient-mapping"))
      .thenReturn(
        FhirMapping(
          id = "test",
          url = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
          name = "test",
          source = Seq(FhirMappingSource(alias = "source", url = "test-source")),
          context = Map.empty,
          mapping = Seq.empty
        )
      )
    mockMappingRepository
  }

  private def getMockSchemaRepository: SchemaFolderRepository = {
    val mockSchemaRepository: SchemaFolderRepository = mock[SchemaFolderRepository]
    when(mockSchemaRepository.getSchema("test-source"))
      .thenReturn(
        Some(StructType(fields = Seq.empty))
      )
    mockSchemaRepository
  }
}
