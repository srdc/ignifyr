package io.ignifyr.server.endpoint

import akka.http.scaladsl.server.Directives.{pathEndOrSingleSlash, pathPrefix}
import akka.http.scaladsl.server.Route
import io.ignifyr.server.common.model.IgnifyrRestCall
import io.ignifyr.server.endpoint.FileSystemTreeStructureEndpoint._
import akka.http.scaladsl.server.Directives._
import io.onfhir.definitions.common.model.Json4sSupport._
import io.ignifyr.server.service.fhir.FileSystemTreeStructureService

class FileSystemTreeStructureEndpoint {

  val service: FileSystemTreeStructureService = new FileSystemTreeStructureService()

  def route(request: IgnifyrRestCall): Route = {
    pathPrefix(SEGMENT_FILE_SYSTEM_PATH) {
      pathEndOrSingleSlash {
        getFolderTreeStructure
      }
    }
  }

  /**
   * Returns the folder tree structure of the file system.
   * @return
   */
  private def getFolderTreeStructure: Route = {
    get {
      parameterMap { queryParams =>
        val basePath = queryParams.getOrElse(QUERY_PARAM_BASE_PATH, ".")
        val includeFiles = queryParams.getOrElse(QUERY_PARAM_INCLUDE_FILES, "false").toBoolean
        complete {
          service.getFolderTreeStructure(basePath, includeFiles)
        }
      }
    }
  }
}

object FileSystemTreeStructureEndpoint {
  val SEGMENT_FILE_SYSTEM_PATH = "file-system-path"
  val QUERY_PARAM_BASE_PATH = "base_path"
  val QUERY_PARAM_INCLUDE_FILES = "include_files"
}
