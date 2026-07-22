package io.ignifyr.server

import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.server.common.config.WebServerConfig
import io.ignifyr.server.common.spi.IgnifyrServerExtensions
import io.ignifyr.server.endpoint.IgnifyrServerEndpoint
import io.onfhir.definitions.resource.fhir.FhirDefinitionsConfig

object IgnifyrServer {
  def start(): Unit = {
    import io.ignifyr.engine.Execution.actorSystem

    val webServerConfig = new WebServerConfig(actorSystem.settings.config.getConfig("webserver"))
    val fhirDefinitionsConfig = new FhirDefinitionsConfig(actorSystem.settings.config.getConfig("fhir"))
    // Discover installed server extension modules (e.g. REDCap); each decides from the root config
    // whether to contribute its routes.
    val serverExtensions = IgnifyrServerExtensions.load(actorSystem.settings.config)
    val endpoint =
      new IgnifyrServerEndpoint(IgnifyrConfig.engineConfig, webServerConfig, fhirDefinitionsConfig, serverExtensions)

    IgnifyrHttpServer.start(endpoint.ignifyrRoute, webServerConfig)
  }
}
