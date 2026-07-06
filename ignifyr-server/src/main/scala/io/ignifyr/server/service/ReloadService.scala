package io.ignifyr.server.service

import io.ignifyr.engine.Execution.actorSystem.dispatcher
import io.ignifyr.server.repository.{FolderDBInitializer, IRepositoryManager}
import io.ignifyr.server.repository.job.JobFolderRepository
import io.ignifyr.server.repository.mapping.ProjectMappingFolderRepository
import io.ignifyr.server.repository.mappingContext.MappingContextFolderRepository
import io.ignifyr.server.repository.schema.SchemaFolderRepository
import io.ignifyr.server.repository.terminology.TerminologySystemFolderRepository

import scala.concurrent.Future

/**
 * Service for reloading resources from the file system.
 */
class ReloadService(repositoryManager: IRepositoryManager) {

  /**
   * Reload all resources.
   *
   * @return
   */
  def reloadResources(): Future[Unit] = {
    Future {
      repositoryManager.mappingRepository.invalidate()
      repositoryManager.schemaRepository.invalidate()
      repositoryManager.mappingJobRepository.invalidate()
      repositoryManager.mappingContextRepository.invalidate()
      repositoryManager.terminologySystemRepository.invalidate()
      repositoryManager.clear()
      repositoryManager.init()
    }
  }
}
