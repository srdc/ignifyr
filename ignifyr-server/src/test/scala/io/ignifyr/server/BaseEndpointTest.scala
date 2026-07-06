package io.ignifyr.server

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.ignifyr.engine.config.IgnifyrEngineConfig
import io.ignifyr.engine.util.FileUtils
import io.ignifyr.server.config.RedCapServiceConfig
import io.ignifyr.server.common.config.WebServerConfig
import io.ignifyr.server.endpoint.{ProjectEndpoint, IgnifyrServerEndpoint}
import io.onfhir.definitions.resource.fhir.FhirDefinitionsConfig
import io.ignifyr.server.model.Project
import io.ignifyr.server.repository.project.ProjectFolderRepository
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.testcontainers.containers.{GenericContainer, MongoDBContainer}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

import java.io.File
import java.time.Duration
import java.util.UUID

trait BaseEndpointTest extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {
  // Ignifyr engine config
  val ignifyrEngineConfig: IgnifyrEngineConfig = new IgnifyrEngineConfig(system.settings.config.getConfig("ignifyr"))
  val webServerConfig = new WebServerConfig(system.settings.config.getConfig("webserver"))
  val fhirDefinitionsConfig = new FhirDefinitionsConfig(system.settings.config.getConfig("fhir"))
  val redCapServiceConfig = new RedCapServiceConfig(system.settings.config.getConfig("ignifyr-redcap"))
  // route endpoint
  var route: Route = _

  /**
   * Identifier of test project which can be used in endpoint tests.
   * Endpoint tests, which require a test project, should call [[createProject]] method to create it
   * */
  var projectId: String = _

  /**
   * Creates a test project whose identifier is stored in [[projectId]].
   * */
  def createProject(id: Option[String] = None): Unit = {
    val project1: Project =
      Project(id = id.getOrElse(UUID.randomUUID().toString), name = "example", description = Some("example project"))
    // create a project
    Post(
      s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}",
      HttpEntity(ContentTypes.`application/json`, writePretty(project1))
    ) ~> route ~> check {
      status shouldEqual StatusCodes.Created
      val project: Project = JsonMethods.parse(responseAs[String]).extract[Project]
      // set the created project
      projectId = project.id
    }
  }

  /**
   * Create the folders and initialize the endpoint and route
   */
  override def beforeAll(): Unit = {
    // Deleting folders to start with a clean environment
    cleanFolders()

    // onfhir needs schema folder to be created in advance,
    // terminology, job folders are created automatically
    FileUtils.getPath(ignifyrEngineConfig.schemaRepositoryFolderPath).toFile.mkdirs()
    // Folder for the mapping repository is also created manually, as the engine's mapping repository requires it during the initialization
    FileUtils.getPath(ignifyrEngineConfig.mappingRepositoryFolderPath).toFile.mkdirs()
    FileUtils.getPath(fhirDefinitionsConfig.profilesPath.get).toFile.mkdirs()
    FileUtils.getPath(fhirDefinitionsConfig.codesystemsPath.get).toFile.mkdirs()
    FileUtils.getPath(fhirDefinitionsConfig.valuesetsPath.get).toFile.mkdirs()
    // initialize endpoint and route
    val endpoint =
      new IgnifyrServerEndpoint(ignifyrEngineConfig, webServerConfig, fhirDefinitionsConfig, Some(redCapServiceConfig))
    route = endpoint.ignifyrRoute
  }

  /**
   * Deletes the repository folders after all test cases are completed.
   * */
  override def afterAll(): Unit = {
    cleanFolders()
  }

  private def cleanFolders(): Unit = {
    // delete projects metadata file if exists
    val projectsJson: File = FileUtils.getPath(ProjectFolderRepository.PROJECTS_JSON).toFile
    if (projectsJson.exists()) {
      org.apache.commons.io.FileUtils.delete(projectsJson)
    }
    org.apache.commons.io.FileUtils
      .deleteDirectory(FileUtils.getPath(ignifyrEngineConfig.terminologySystemFolderPath).toFile)
    org.apache.commons.io.FileUtils
      .deleteDirectory(FileUtils.getPath(ignifyrEngineConfig.schemaRepositoryFolderPath).toFile)
    org.apache.commons.io.FileUtils
      .deleteDirectory(FileUtils.getPath(ignifyrEngineConfig.jobRepositoryFolderPath).toFile)
    org.apache.commons.io.FileUtils
      .deleteDirectory(FileUtils.getPath(ignifyrEngineConfig.mappingRepositoryFolderPath).toFile)
    org.apache.commons.io.FileUtils
      .deleteDirectory(FileUtils.getPath(ignifyrEngineConfig.mappingContextRepositoryFolderPath).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(fhirDefinitionsConfig.profilesPath.get).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(fhirDefinitionsConfig.codesystemsPath.get).toFile)
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath(fhirDefinitionsConfig.valuesetsPath.get).toFile)
  }
}
