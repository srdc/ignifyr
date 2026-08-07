package io.ignifyr.server.repository

import io.onfhir.definitions.common.model.SchemaDefinition
import io.ignifyr.engine.model.{FhirMapping, FhirMappingJob, FhirMappingSource, FhirRepositorySinkSettings}
import io.ignifyr.engine.util.FileUtils
import io.ignifyr.server.model.Project
import io.ignifyr.server.repository.job.JobFolderRepository
import io.ignifyr.server.repository.mapping.ProjectMappingFolderRepository
import io.ignifyr.server.repository.mappingContext.MappingContextFolderRepository
import io.ignifyr.server.repository.project.ProjectFolderRepository
import io.ignifyr.server.repository.schema.SchemaFolderRepository
import org.mockito.ArgumentCaptor
import org.mockito.MockitoSugar._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.concurrent.Future

/**
 * Server startup: the project index is either read back from `projects.json` or, when that file is gone,
 * rebuilt by scanning the repository folders. Both paths run before the first request is served, so a
 * failure here is "the server does not start" rather than a failing call — and the rebuild path is the
 * only recovery there is if the index file is ever lost.
 *
 * The repositories are mocked so the initializer's own resolution logic is what is under test, not the
 * folder repositories it delegates to.
 */
class FolderDBInitializerTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val projectId = "project-1"

  private val schema = SchemaDefinition(
    id = "schema-1",
    url = "https://ignifyr.io/fhir/StructureDefinition/Ext-patient",
    version = SchemaDefinition.VERSION_LATEST,
    `type` = "Ext-patient",
    name = "ext-patient",
    description = None,
    rootDefinition = None,
    fieldDefinitions = None
  )

  private val mapping = FhirMapping(
    id = "mapping-1",
    url = "https://ignifyr.io/fhir/mappings/patient-mapping",
    name = "patient-mapping",
    source = Seq(FhirMappingSource(alias = "source", url = schema.url)),
    context = Map.empty,
    mapping = Seq.empty
  )

  private val job = FhirMappingJob(
    id = "job-1",
    sourceSettings = Map.empty,
    sinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost/fhir"),
    mappings = Seq.empty
  )

  private def projectsJsonFile = FileUtils.getPath(ProjectFolderRepository.PROJECTS_JSON).toFile

  private def writeProjectsJson(content: String): Unit = {
    projectsJsonFile.getParentFile.mkdirs()
    Files.write(projectsJsonFile.toPath, content.getBytes(StandardCharsets.UTF_8))
  }

  override def beforeEach(): Unit = if (projectsJsonFile.exists()) projectsJsonFile.delete()
  override def afterEach(): Unit = if (projectsJsonFile.exists()) projectsJsonFile.delete()

  /**
   * Builds an initializer over mocked repositories, returning it with the project repository to verify.
   * The `*ById` maps stub what each repository answers for a resource id the index file references; an
   * id with no entry resolves to None, which is the "file is gone" case.
   */
  private def initializerOver(
      schemas: Map[String, Seq[SchemaDefinition]] = Map.empty,
      mappings: Map[String, Seq[FhirMapping]] = Map.empty,
      jobs: Map[String, Seq[FhirMappingJob]] = Map.empty,
      contexts: Map[String, Seq[String]] = Map.empty,
      schemasById: Map[String, Option[SchemaDefinition]] = Map.empty,
      mappingsById: Map[String, Option[FhirMapping]] = Map.empty,
      jobsById: Map[String, Option[FhirMappingJob]] = Map.empty
  ): (FolderDBInitializer, ProjectFolderRepository) = {
    val projectRepository = mock[ProjectFolderRepository]

    val schemaRepository = mock[SchemaFolderRepository]
    when(schemaRepository.getProjectPairs).thenReturn(schemas)
    schemasById.foreach { case (id, answer) =>
      when(schemaRepository.getSchema(projectId, id)).thenReturn(Future.successful(answer))
    }

    val mappingRepository = mock[ProjectMappingFolderRepository]
    when(mappingRepository.getProjectPairs).thenReturn(mappings)
    mappingsById.foreach { case (id, answer) =>
      when(mappingRepository.getMapping(projectId, id)).thenReturn(Future.successful(answer))
    }

    val jobRepository = mock[JobFolderRepository]
    when(jobRepository.getProjectPairs).thenReturn(jobs)
    jobsById.foreach { case (id, answer) =>
      when(jobRepository.getJob(projectId, id)).thenReturn(Future.successful(answer))
    }

    val contextRepository = mock[MappingContextFolderRepository]
    when(contextRepository.getProjectPairs).thenReturn(contexts)

    (
      new FolderDBInitializer(projectRepository, schemaRepository, mappingRepository, jobRepository, contextRepository),
      projectRepository
    )
  }

  /** The projects the initializer handed to the repository. */
  private def injectedProjects(projectRepository: ProjectFolderRepository): Map[String, Project] = {
    val captor: ArgumentCaptor[Map[String, Project]] =
      ArgumentCaptor.forClass(classOf[Map[String, Project]])
    verify(projectRepository).setProjects(captor.capture())
    captor.getValue
  }

  "init" should "rebuild the project index from the repository folders when there is no index file" in {
    val (initializer, projectRepository) = initializerOver(
      schemas = Map(projectId -> Seq(schema)),
      mappings = Map(projectId -> Seq(mapping)),
      jobs = Map(projectId -> Seq(job)),
      contexts = Map(projectId -> Seq("unit-conversion"))
    )
    initializer.init()

    val projects = injectedProjects(projectRepository)
    projects.keySet shouldBe Set(projectId)
    val project = projects(projectId)
    project.schemas.map(_.id) shouldBe Seq("schema-1")
    project.mappings.map(_.id) shouldBe Seq("mapping-1")
    project.mappingJobs.map(_.id) shouldBe Seq("job-1")
    project.mappingContexts shouldBe Seq("unit-conversion")
  }

  it should "write the index file it did not find, so the rebuild happens only once" in {
    val (initializer, _) = initializerOver()
    projectsJsonFile should not(exist)
    initializer.init()
    projectsJsonFile should exist
  }

  // With no index file there is no project name either, so the folder name stands in for it.
  it should "name a rebuilt project after its folder and derive the url prefixes from its resources" in {
    val (initializer, projectRepository) = initializerOver(
      schemas = Map(projectId -> Seq(schema)),
      mappings = Map(projectId -> Seq(mapping))
    )
    initializer.init()

    val project = injectedProjects(projectRepository)(projectId)
    project.name shouldBe projectId
    project.schemaUrlPrefix shouldBe Some("https://ignifyr.io/fhir/StructureDefinition/")
    project.mappingUrlPrefix shouldBe Some("https://ignifyr.io/fhir/mappings/")
  }

  it should "collect a project that owns only some of the resource kinds" in {
    val (initializer, projectRepository) = initializerOver(
      schemas = Map("only-schemas" -> Seq(schema)),
      jobs = Map("only-jobs" -> Seq(job))
    )
    initializer.init()

    val projects = injectedProjects(projectRepository)
    projects.keySet shouldBe Set("only-schemas", "only-jobs")
    projects("only-schemas").mappingJobs shouldBe empty
    projects("only-jobs").schemas shouldBe empty
  }

  it should "resolve the resources referenced by an existing index file" in {
    writeProjectsJson(s"""[{
         |  "id": "$projectId",
         |  "name": "Example project",
         |  "description": "an example",
         |  "schemaUrlPrefix": "https://ignifyr.io/fhir/StructureDefinition/",
         |  "mappingUrlPrefix": "https://ignifyr.io/fhir/mappings/",
         |  "mappingContexts": ["unit-conversion"],
         |  "schemas": [{"id": "schema-1"}],
         |  "mappings": [{"id": "mapping-1"}],
         |  "mappingJobs": [{"id": "job-1"}]
         |}]""".stripMargin)

    val (initializer, projectRepository) = initializerOver(
      schemasById = Map("schema-1" -> Some(schema)),
      mappingsById = Map("mapping-1" -> Some(mapping)),
      jobsById = Map("job-1" -> Some(job))
    )
    initializer.init()

    val project = injectedProjects(projectRepository)(projectId)
    project.name shouldBe "Example project"
    project.description shouldBe Some("an example")
    project.schemas.map(_.id) shouldBe Seq("schema-1")
    project.mappings.map(_.id) shouldBe Seq("mapping-1")
    project.mappingJobs.map(_.id) shouldBe Seq("job-1")
    project.mappingContexts shouldBe Seq("unit-conversion")
  }

  it should "read an index file that lists a project with no resources" in {
    writeProjectsJson(s"""[{
         |  "id": "$projectId", "name": "Empty", "mappingContexts": [],
         |  "schemas": [], "mappings": [], "mappingJobs": []
         |}]""".stripMargin)

    val (initializer, projectRepository) = initializerOver()
    initializer.init()

    val project = injectedProjects(projectRepository)(projectId)
    project.schemas shouldBe empty
    project.description shouldBe None
  }

  /*
   * The realistic corruption: somebody deletes a mapping file by hand, leaving the index pointing at it.
   * The initializer refuses to start rather than silently serving a project with a hole in it — the
   * failure must name the id so the operator can find the missing file.
   */
  it should "refuse to start when the index references a mapping that is not on disk" in {
    writeProjectsJson(s"""[{
         |  "id": "$projectId", "name": "Example", "mappingContexts": [],
         |  "schemas": [], "mappings": [{"id": "mapping-gone"}], "mappingJobs": []
         |}]""".stripMargin)

    val (initializer, _) = initializerOver(mappingsById = Map("mapping-gone" -> None))
    val thrown = the[IllegalStateException] thrownBy initializer.init()
    thrown.getMessage should include("mapping-gone")
  }

  it should "refuse to start when the index references a schema that is not on disk" in {
    writeProjectsJson(s"""[{
         |  "id": "$projectId", "name": "Example", "mappingContexts": [],
         |  "schemas": [{"id": "schema-gone"}], "mappings": [], "mappingJobs": []
         |}]""".stripMargin)

    val (initializer, _) = initializerOver(schemasById = Map("schema-gone" -> None))
    val thrown = the[IllegalStateException] thrownBy initializer.init()
    thrown.getMessage should include("schema-gone")
  }

  it should "refuse to start when the index references a job that is not on disk" in {
    writeProjectsJson(s"""[{
         |  "id": "$projectId", "name": "Example", "mappingContexts": [],
         |  "schemas": [], "mappings": [], "mappingJobs": [{"id": "job-gone"}]
         |}]""".stripMargin)

    val (initializer, _) = initializerOver(jobsById = Map("job-gone" -> None))
    val thrown = the[IllegalStateException] thrownBy initializer.init()
    thrown.getMessage should include("job-gone")
  }

  "removeProjectsJsonFile" should "delete the index file, and do nothing when it is already gone" in {
    val (initializer, _) = initializerOver()
    writeProjectsJson("[]")
    projectsJsonFile should exist

    initializer.removeProjectsJsonFile()
    projectsJsonFile should not(exist)

    noException should be thrownBy initializer.removeProjectsJsonFile()
  }
}
