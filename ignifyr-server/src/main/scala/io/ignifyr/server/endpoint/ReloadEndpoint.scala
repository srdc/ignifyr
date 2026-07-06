package io.ignifyr.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, get, pathEndOrSingleSlash, pathPrefix}
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.ignifyr.engine.Execution.actorSystem.dispatcher
import io.ignifyr.server.common.model.IgnifyrRestCall
import io.ignifyr.server.endpoint.ReloadEndpoint.SEGMENT_RELOAD
import io.ignifyr.server.repository.{FolderDBInitializer, IRepositoryManager}
import io.ignifyr.server.repository.job.JobFolderRepository
import io.ignifyr.server.repository.mapping.ProjectMappingFolderRepository
import io.ignifyr.server.repository.mappingContext.MappingContextFolderRepository
import io.ignifyr.server.repository.schema.SchemaFolderRepository
import io.ignifyr.server.repository.terminology.TerminologySystemFolderRepository
import io.ignifyr.server.service.ReloadService

/**
 * Endpoint to reload resources from the file system.
 * */
class ReloadEndpoint(repositoryManager: IRepositoryManager) extends LazyLogging {

  val reloadService: ReloadService = new ReloadService(repositoryManager)

  def route(request: IgnifyrRestCall): Route = {
    pathPrefix(SEGMENT_RELOAD) {
      pathEndOrSingleSlash {
        reloadResources
      }
    }
  }

  /**
   * Route to reload all resources
   * @return
   */
  private def reloadResources: Route = {
    get {
      complete {
        reloadService.reloadResources() map { _ =>
          StatusCodes.NoContent
        }
      }
    }
  }
}

object ReloadEndpoint {
  val SEGMENT_RELOAD = "reload"
}
