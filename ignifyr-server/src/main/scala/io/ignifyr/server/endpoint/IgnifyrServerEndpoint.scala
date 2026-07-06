package io.ignifyr.server.endpoint

import akka.http.scaladsl.model.{HttpMethod, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RejectionHandler, Route}
import io.ignifyr.engine.config.IgnifyrEngineConfig
import io.ignifyr.server.config.RedCapServiceConfig
import io.ignifyr.server.common.config.WebServerConfig
import io.onfhir.definitions.resource.fhir.FhirDefinitionsConfig
import io.onfhir.definitions.resource.endpoint.FhirDefinitionsEndpoint
import io.ignifyr.server.common.interceptor.{ICORSHandler, IErrorHandler}
import io.ignifyr.server.common.model.IgnifyrRestCall
import io.ignifyr.server.repository.job.JobFolderRepository
import io.ignifyr.server.repository.mapping.ProjectMappingFolderRepository
import io.ignifyr.server.repository.mappingContext.MappingContextFolderRepository
import io.ignifyr.server.repository.project.{IProjectRepository, ProjectFolderRepository}
import io.ignifyr.server.repository.schema.SchemaFolderRepository
import io.ignifyr.server.repository.terminology.{ITerminologySystemRepository, TerminologySystemFolderRepository}
import io.ignifyr.server.repository.terminology.codesystem.{CodeSystemFolderRepository, ICodeSystemRepository}
import io.ignifyr.server.repository.terminology.conceptmap.{ConceptMapFolderRepository, IConceptMapRepository}
import io.ignifyr.server.util.IgnifyrRejectionHandler
import io.onfhir.definitions.fhirpath.endpoint.FhirPathFunctionsEndpoint
import io.ignifyr.server.repository.{FolderDBInitializer, FolderRepositoryManager, IRepositoryManager}

import java.util.UUID

/**
 * Encapsulates all services and directives
 * Main Endpoint for Ignifyr server
 */
class IgnifyrServerEndpoint(
    ignifyrEngineConfig: IgnifyrEngineConfig,
    webServerConfig: WebServerConfig,
    fhirDefinitionsConfig: FhirDefinitionsConfig,
    redCapServiceConfig: Option[RedCapServiceConfig]
) extends ICORSHandler
    with IErrorHandler {

  private val repositoryManager: IRepositoryManager = new FolderRepositoryManager(ignifyrEngineConfig)
  // Initialize repositories by reading the resources available in the file system
  repositoryManager.init()

  private val projectEndpoint = new ProjectEndpoint(
    repositoryManager.schemaRepository,
    repositoryManager.mappingRepository,
    repositoryManager.mappingJobRepository,
    repositoryManager.mappingContextRepository,
    repositoryManager.projectRepository
  )

  val terminologyServiceManagerEndpoint = new TerminologyServiceManagerEndpoint(
    repositoryManager.terminologySystemRepository,
    repositoryManager.conceptMapRepository,
    repositoryManager.codeSystemRepository,
    repositoryManager.mappingJobRepository
  )

  val fhirDefinitionsEndpoint = new FhirDefinitionsEndpoint(fhirDefinitionsConfig)

  val functionLibraryPackages: Seq[String] =
    Seq("io.onfhir.path", "io.ignifyr.engine.mapping") ++
      ignifyrEngineConfig.functionLibrariesConfig // add external function libraries
        .map(_.libraryPackageNames)
        .getOrElse(Seq.empty)
  val fhirPathFunctionsEndpoint = new FhirPathFunctionsEndpoint(functionLibraryPackages)

  val redcapEndpoint = redCapServiceConfig.map(config => new RedCapEndpoint(config))
  val fileSystemTreeStructureEndpoint = new FileSystemTreeStructureEndpoint()
  val metadataEndpoint =
    new MetadataEndpoint(ignifyrEngineConfig, webServerConfig, fhirDefinitionsConfig, redCapServiceConfig)

  val reloadEndpoint = new ReloadEndpoint(repositoryManager)

  // Custom rejection handler to send proper messages to user
  val ignifyrRejectionHandler: RejectionHandler = IgnifyrRejectionHandler.getRejectionHandler()

  lazy val ignifyrRoute: Route =
    pathPrefix(webServerConfig.baseUri) {
      corsHandler {
        withRequestTimeoutResponse(_ => IgnifyrRejectionHandler.timeoutResponseHandler()) {
          extractMethod { httpMethod: HttpMethod =>
            extractUri { requestUri: Uri =>
              extractRequestEntity { requestEntity =>
                optionalHeaderValueByName("X-Correlation-Id") { correlationId =>
                  val restCall = new IgnifyrRestCall(
                    method = httpMethod,
                    uri = requestUri,
                    requestId = correlationId.getOrElse(UUID.randomUUID().toString),
                    requestEntity = requestEntity
                  )
                  handleRejections(ignifyrRejectionHandler) {
                    handleExceptions(exceptionHandler(restCall)) { // Handle exceptions
                      // RedCap Endpoint is optional, so it will be handled separately
                      val routes = Seq(
                        terminologyServiceManagerEndpoint.route(restCall),
                        projectEndpoint.route(restCall),
                        fhirDefinitionsEndpoint.route(),
                        fhirPathFunctionsEndpoint.route(),
                        fileSystemTreeStructureEndpoint.route(restCall),
                        metadataEndpoint.route(restCall),
                        reloadEndpoint.route(restCall)
                      ) ++ redcapEndpoint.map(_.route(restCall))

                      concat(routes: _*)
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
}
