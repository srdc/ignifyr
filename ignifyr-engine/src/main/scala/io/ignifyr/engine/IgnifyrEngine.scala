package io.ignifyr.engine

import io.onfhir.path._
import io.ignifyr.engine.config.{IgnifyrConfig, IgnifyrEngineConfig}
import io.ignifyr.engine.execution.RunningJobRegistry
import io.ignifyr.engine.execution.processing.FileStreamInputArchiver
import io.ignifyr.engine.mapping.context.{IMappingContextLoader, MappingContextLoader}
import io.ignifyr.engine.mapping.schema.{IFhirSchemaLoader, SchemaFolderLoader}
import io.ignifyr.engine.model.exception.EngineInitializationException
import io.ignifyr.engine.repository.mapping.{FhirMappingFolderRepository, IFhirMappingRepository}
import io.ignifyr.engine.spi.ExtensionRegistry
import io.ignifyr.engine.util.FileUtils
import org.apache.spark.sql.SparkSession

/**
 * <p>Ignifyr Engine for executing mapping jobs and tasks.</p>
 * <p>During initialization, the engine prioritizes the mapping and schema repositories provided as a constructor parameter.
 * If they are not provided as constructor parameters, they are initialized as folder repository based on the folder paths set in the engine configurations.
 * </p>
 *
 * @param mappingRepository        Already instantiated mapping repository that maintains a dynamically-updated data structure based on the operations on the mappings
 * @param schemaRepository         Already instantiated schema repository that maintains a dynamically-updated data structure based on the operations on the schemas
 */
class IgnifyrEngine(
    mappingRepository: Option[IFhirMappingRepository] = None,
    schemaRepository: Option[IFhirSchemaLoader] = None
) {
  // Validate that both mapping and schema repositories are empty or non-empty
  if (
    mappingRepository.nonEmpty && schemaRepository.isEmpty || mappingRepository.isEmpty && schemaRepository.nonEmpty
  ) {
    throw EngineInitializationException("Mapping and schema repositories should both empty or non-empty")
  }

  val engineConfig: IgnifyrEngineConfig = IgnifyrConfig.engineConfig

  val sparkSession: SparkSession = IgnifyrConfig.sparkSession

  // Discover installed extension modules (connectors, sinks, services, CLI commands) and validate
  // their registrations up front, so duplicate/misconfigured plugins fail at startup, not mid-job.
  ExtensionRegistry.init()

  // Repository for mapping definitions
  val mappingRepo: IFhirMappingRepository = mappingRepository.getOrElse(
    new FhirMappingFolderRepository(FileUtils.getPath(engineConfig.mappingRepositoryFolderPath).toUri)
  )

  // Context loader
  val contextLoader: IMappingContextLoader = new MappingContextLoader

  // Repository for source data schemas
  val schemaLoader: IFhirSchemaLoader =
    schemaRepository.getOrElse(new SchemaFolderLoader(FileUtils.getPath(engineConfig.schemaRepositoryFolderPath).toUri))

  // Function libraries containing context-independent, built-in libraries and libraries passed externally
  val functionLibraries: Map[String, IFhirPathFunctionLibraryFactory] = initializeFunctionLibraries()

  // Single registry keeping the running jobs
  val runningJobRegistry: RunningJobRegistry = new RunningJobRegistry(sparkSession)

  // Archiver for deleting or archiving the files processed
  val fileStreamInputArchiver: FileStreamInputArchiver = new FileStreamInputArchiver(runningJobRegistry)
  // Only run the streaming input-archiver timer when a streaming runtime is installed; the community
  // batch-only engine has no streaming jobs to archive, so the timer would just spin idle.
  if (ExtensionRegistry.streaming.isDefined) fileStreamInputArchiver.startStreamingArchiveTask()

  /**
   * Merges built-in function libraries and external libraries passed in the constructor
   *
   * @return
   */
  private def initializeFunctionLibraries(): Map[String, IFhirPathFunctionLibraryFactory] = {
    val externalFunctionLibraryFactories: Map[String, IFhirPathFunctionLibraryFactory] =
      engineConfig.functionLibrariesConfig
        .map(_.functionLibrariesFactories)
        .getOrElse(Map.empty)
    Map(
      FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory,
      FhirPathNavFunctionsFactory.defaultPrefix -> FhirPathNavFunctionsFactory,
      FhirPathAggFunctionsFactory.defaultPrefix -> FhirPathAggFunctionsFactory,
      FhirPathIdentityServiceFunctionsFactory.defaultPrefix -> FhirPathIdentityServiceFunctionsFactory,
      FhirPathTerminologyServiceFunctionsFactory.defaultPrefix -> FhirPathTerminologyServiceFunctionsFactory
    ) ++ externalFunctionLibraryFactories
  }
}
