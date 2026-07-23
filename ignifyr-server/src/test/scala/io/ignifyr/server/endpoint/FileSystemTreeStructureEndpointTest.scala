package io.ignifyr.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import io.ignifyr.engine.util.FileUtils
import io.ignifyr.server.BaseEndpointTest
import io.ignifyr.server.endpoint.FileSystemTreeStructureEndpoint
import io.ignifyr.server.model.FilePathNode
import io.onfhir.definitions.common.model.Json4sSupport.formats
import org.json4s.jackson.JsonMethods

import java.nio.file.{Files, Path}

class FileSystemTreeStructureEndpointTest extends BaseEndpointTest {

  /**
   * A hermetic fixture tree the suite creates and destroys itself, rather than asserting against the
   * shared context root (`base_path=.`). The endpoint rejects any base path outside the context root,
   * so the fixture is created *inside* it — but with a unique name and a known structure, so the
   * asserted counts stay deterministic regardless of the transient folders (repository dirs, logs,
   * checkpoints, …) other suites leave in the context root during a full reactor run.
   *
   * Layout: {{{
   *   <fixture>/
   *     alpha/            (folder, with a file inside → still leaf when files are excluded)
   *       data.csv
   *     beta/             (folder, empty)
   *     gamma/            (folder, empty)
   *     projects.json     (file)
   * }}}
   */
  private var fixtureRoot: Path = _
  private var fixtureBasePath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val contextRoot = FileUtils.getPath("").toAbsolutePath
    Files.createDirectories(contextRoot)
    fixtureRoot = Files.createTempDirectory(contextRoot, "fs-tree-test-")
    // base_path is resolved relative to the context root, so hand the endpoint the relative name.
    fixtureBasePath = contextRoot.relativize(fixtureRoot).toString.replaceAll("\\\\", "/")

    val alpha = Files.createDirectory(fixtureRoot.resolve("alpha"))
    Files.createDirectory(fixtureRoot.resolve("beta"))
    Files.createDirectory(fixtureRoot.resolve("gamma"))
    Files.createFile(fixtureRoot.resolve("projects.json"))
    Files.createFile(alpha.resolve("data.csv"))
  }

  override def afterAll(): Unit = {
    if (fixtureRoot != null) {
      org.apache.commons.io.FileUtils.deleteDirectory(fixtureRoot.toFile)
    }
    super.afterAll()
  }

  "File system tree structure endpoint" should {

    "retrieve all folders (only directories) under the given base path" in {
      Get(
        s"/${webServerConfig.baseUri}/${FileSystemTreeStructureEndpoint.SEGMENT_FILE_SYSTEM_PATH}?${FileSystemTreeStructureEndpoint.QUERY_PARAM_BASE_PATH}=$fixtureBasePath&${FileSystemTreeStructureEndpoint.QUERY_PARAM_INCLUDE_FILES}=false"
      ) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val fileNode = JsonMethods.parse(responseAs[String]).extract[FilePathNode]
        fileNode.isFolder shouldEqual true
        // Only the three folders; projects.json and alpha/data.csv are excluded when include_files=false.
        fileNode.children.map(_.label).sorted shouldBe List("alpha", "beta", "gamma")
        fileNode.children.foreach { child =>
          child.isFolder shouldBe true
          child.children shouldBe empty // files inside a folder are excluded, so every leaf is empty
        }
      }
    }

    "retrieve all folders including all files under the given base path" in {
      Get(
        s"/${webServerConfig.baseUri}/${FileSystemTreeStructureEndpoint.SEGMENT_FILE_SYSTEM_PATH}?${FileSystemTreeStructureEndpoint.QUERY_PARAM_BASE_PATH}=$fixtureBasePath&${FileSystemTreeStructureEndpoint.QUERY_PARAM_INCLUDE_FILES}=true"
      ) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val fileNode = JsonMethods.parse(responseAs[String]).extract[FilePathNode]
        fileNode.isFolder shouldEqual true
        fileNode.children.length shouldBe 4 // three folders + projects.json
        fileNode.children.count(_.label == "projects.json") shouldBe 1
        fileNode.children.count(_.isFolder) shouldBe 3
        // Files are now included during recursion into a folder.
        fileNode.children.find(_.label == "alpha").flatMap(_.children.headOption).map(_.label) shouldBe Some("data.csv")
      }
    }

  }

}
