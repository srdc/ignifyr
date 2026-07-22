package io.ignifyr.server.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.onfhir.definitions.common.model.Json4sSupport._
import io.ignifyr.engine.config.IgnifyrEngineConfig
import io.ignifyr.server.common.config.WebServerConfig
import io.ignifyr.server.common.model.IgnifyrRestCall
import io.ignifyr.server.common.spi.IgnifyrServerExtension
import io.ignifyr.server.endpoint.MetadataEndpoint.SEGMENT_METADATA
import io.onfhir.definitions.resource.fhir.FhirDefinitionsConfig
import io.ignifyr.server.service.MetadataService

/**
 * Endpoint to return metadata of the server.
 * */
class MetadataEndpoint(
    ignifyrEngineConfig: IgnifyrEngineConfig,
    webServerConfig: WebServerConfig,
    fhirDefinitionsConfig: FhirDefinitionsConfig,
    serverExtensions: Seq[IgnifyrServerExtension]
) extends LazyLogging {

  val service: MetadataService = new MetadataService(
    ignifyrEngineConfig,
    webServerConfig,
    fhirDefinitionsConfig,
    serverExtensions
  )

  def route(request: IgnifyrRestCall): Route = {
    pathPrefix(SEGMENT_METADATA) {
      pathEndOrSingleSlash {
        getMetadata
      }
    }
  }

  /**
   * Returns the documentations of FhirPath functions.
   * */
  private def getMetadata: Route = {
    get {
      complete {
        service.getMetadata
      }
    }
  }
}

object MetadataEndpoint {
  val SEGMENT_METADATA = "metadata"
}
