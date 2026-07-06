package io.ignifyr.server.service

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.Unmarshal
import io.ignifyr.engine.config.IgnifyrEngineConfig
import io.ignifyr.server.common.config.WebServerConfig
import io.ignifyr.server.config.RedCapServiceConfig
import io.onfhir.definitions.resource.fhir.FhirDefinitionsConfig
import io.ignifyr.engine.Execution.actorSystem
import io.ignifyr.engine.Execution.actorSystem.dispatcher
import io.ignifyr.engine.env.EnvironmentVariableResolver
import io.ignifyr.server.endpoint.MetadataEndpoint.SEGMENT_METADATA
import io.ignifyr.server.model.{Archiving, MappingExecutionConfiguration, Metadata, RepositoryNames}

import java.util.Properties
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

/**
 * Service for retrieving metadata about the Ignifyr server.
 * @param ignifyrEngineConfig engine related configurations
 * @param webServerConfig web server related configurations
 * @param fhirDefinitionsConfig fhir related configurations
 * @param redCapServiceConfig redcap service related configurations
 */
class MetadataService(
    ignifyrEngineConfig: IgnifyrEngineConfig,
    webServerConfig: WebServerConfig,
    fhirDefinitionsConfig: FhirDefinitionsConfig,
    redCapServiceConfig: Option[RedCapServiceConfig]
) {

  /**
   * Use configurations to create a Metadata object along with the version set in pom.xml.
   * @return
   */
  def getMetadata: Metadata = {
    val properties: Properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("version.properties"))
    val ignifyrRedCapVersion = getIgnifyrRedCapVersion
    // fetch the mapping executions' configurations
    val configurations: Seq[MappingExecutionConfiguration] = getMappingExecutionConfigurations
    Metadata(
      name = "Ignifyr",
      description = "Ignifyr is a tool for mapping data from various sources to FHIR resources.",
      version = properties.getProperty("application.version"),
      fhirDefinitionsVersion = fhirDefinitionsConfig.majorFhirVersion,
      ignifyrRedcapVersion = ignifyrRedCapVersion,
      definitionsRootUrls = fhirDefinitionsConfig.definitionsRootURLs,
      schemasFhirVersion = ignifyrEngineConfig.schemaRepositoryFhirVersion,
      repositoryNames = RepositoryNames(
        mappings = ignifyrEngineConfig.mappingRepositoryFolderPath,
        schemas = ignifyrEngineConfig.schemaRepositoryFolderPath,
        contexts = ignifyrEngineConfig.mappingContextRepositoryFolderPath,
        jobs = ignifyrEngineConfig.jobRepositoryFolderPath,
        terminologySystems = ignifyrEngineConfig.terminologySystemFolderPath
      ),
      archiving = Archiving(
        erroneousRecordsFolder = ignifyrEngineConfig.erroneousRecordsFolder,
        archiveFolder = ignifyrEngineConfig.archiveFolder,
        streamArchivingFrequency = ignifyrEngineConfig.streamArchivingFrequency
      ),
      environmentVariables = EnvironmentVariableResolver.getEnvironmentVariables,
      executionConfigurations = configurations
    )
  }

  /**
   * Try to connect to the ignifyr-redcap service to get the version of the Ignifyr redcap version.
   * If no response is received, return None.
   * @return
   */
  private def getIgnifyrRedCapVersion: Option[String] = {
    redCapServiceConfig.flatMap { redCapConfig =>
      val proxiedRequest = HttpRequest(
        method = HttpMethods.GET,
        uri = s"${redCapConfig.endpoint}/$SEGMENT_METADATA",
        headers = RawHeader("Content-Type", "application/json") :: Nil
      )

      val responseFuture: Future[HttpResponse] = Http().singleRequest(proxiedRequest)
      val responseAsString = Try(
        Await.result(
          responseFuture.flatMap(resp => Unmarshal(resp.entity).to[String]),
          1.seconds // increasing this leads to increase initial loading time of the Ignifyr frontend
        )
      )

      responseAsString match {
        case Success(res) => Some(res)
        case Failure(_) => None
      }
    }
  }

  /**
   * Retrieves a sequence of predefined configurations used during the execution of mapping jobs.
   *
   * @return A sequence of `Configuration` objects, each representing a specific setting.
   */
  private def getMappingExecutionConfigurations: Seq[MappingExecutionConfiguration] = {
    Seq(
      MappingExecutionConfiguration(
        name = "Mapping Timeout",
        description = "Timeout for each mapping execution on an individual input record",
        value = ignifyrEngineConfig.mappingTimeout.toString
      ),
      MappingExecutionConfiguration(
        name = "Maximum Chunk Size",
        description =
          "Max chunk size to execute for batch executions, if number of records exceed this, the source data will be divided into chunks",
        value = ignifyrEngineConfig.maxChunkSizeForMappingJobs.getOrElse("Not Set").toString
      ),
      MappingExecutionConfiguration(
        name = "Batch Group Size",
        description =
          "The number of FHIR resources in the group while executing (create/update) a FHIR batch operation.",
        value = ignifyrEngineConfig.fhirWriterBatchGroupSize.toString
      )
    )
  }
}
