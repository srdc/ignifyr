package io.ignifyr.server.repository

import io.ignifyr.server.repository.job.IJobRepository
import io.ignifyr.server.repository.mapping.IMappingRepository
import io.ignifyr.server.repository.mappingContext.IMappingContextRepository
import io.ignifyr.server.repository.project.IProjectRepository
import io.ignifyr.server.repository.schema.ISchemaRepository
import io.ignifyr.server.repository.terminology.ITerminologySystemRepository
import io.ignifyr.server.repository.terminology.codesystem.ICodeSystemRepository
import io.ignifyr.server.repository.terminology.conceptmap.IConceptMapRepository

/**
 * Manage the repositories throughout Ignifyr
 */
trait IRepositoryManager {
  val projectRepository: IProjectRepository
  val mappingRepository: IMappingRepository
  val schemaRepository: ISchemaRepository
  val mappingJobRepository: IJobRepository
  val mappingContextRepository: IMappingContextRepository

  val terminologySystemRepository: ITerminologySystemRepository
  val conceptMapRepository: IConceptMapRepository
  val codeSystemRepository: ICodeSystemRepository

  /**
   * Initialize the repository
   */
  def init(): Unit

  /**
   * Clean-up the repository database
   */
  def clear(): Unit
}
