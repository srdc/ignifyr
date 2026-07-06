package io.ignifyr.server.repository

import io.ignifyr.engine.config.IgnifyrEngineConfig
import io.ignifyr.server.repository.job.JobFolderRepository
import io.ignifyr.server.repository.mapping.ProjectMappingFolderRepository
import io.ignifyr.server.repository.mappingContext.MappingContextFolderRepository
import io.ignifyr.server.repository.project.ProjectFolderRepository
import io.ignifyr.server.repository.schema.SchemaFolderRepository
import io.ignifyr.server.repository.terminology.TerminologySystemFolderRepository
import io.ignifyr.server.repository.terminology.codesystem.CodeSystemFolderRepository
import io.ignifyr.server.repository.terminology.conceptmap.ConceptMapFolderRepository

/**
 * Folder/file based implementation of the RepositoryManager where all managed repositories are folder-based.
 *
 * @param ignifyrEngineConfig
 */
class FolderRepositoryManager(ignifyrEngineConfig: IgnifyrEngineConfig) extends IRepositoryManager {

  override val projectRepository: ProjectFolderRepository = new ProjectFolderRepository(ignifyrEngineConfig)
  override val mappingRepository: ProjectMappingFolderRepository =
    new ProjectMappingFolderRepository(ignifyrEngineConfig.mappingRepositoryFolderPath, projectRepository)
  override val schemaRepository: SchemaFolderRepository =
    new SchemaFolderRepository(ignifyrEngineConfig.schemaRepositoryFolderPath, projectRepository)
  override val mappingJobRepository: JobFolderRepository =
    new JobFolderRepository(ignifyrEngineConfig.jobRepositoryFolderPath, projectRepository)
  override val mappingContextRepository: MappingContextFolderRepository =
    new MappingContextFolderRepository(ignifyrEngineConfig.mappingContextRepositoryFolderPath, projectRepository)

  override val terminologySystemRepository: TerminologySystemFolderRepository = new TerminologySystemFolderRepository(
    ignifyrEngineConfig.terminologySystemFolderPath
  )
  override val conceptMapRepository: ConceptMapFolderRepository = new ConceptMapFolderRepository(
    ignifyrEngineConfig.terminologySystemFolderPath
  )
  override val codeSystemRepository: CodeSystemFolderRepository = new CodeSystemFolderRepository(
    ignifyrEngineConfig.terminologySystemFolderPath
  )

  private val folderDBInitializer = new FolderDBInitializer(
    projectRepository,
    schemaRepository,
    mappingRepository,
    mappingJobRepository,
    mappingContextRepository
  )

  /**
   * Initializes the Repository Manager's internal database (the projects.json file) after initialization of
   * each individual repository.
   */
  override def init(): Unit = {
    folderDBInitializer.init()
  }

  /**
   * Deletes the internal repository database (the projects.json file) for a fresh start (usually after cache invalidate operations)
   */
  override def clear(): Unit = {
    folderDBInitializer.removeProjectsJsonFile()
  }

}
