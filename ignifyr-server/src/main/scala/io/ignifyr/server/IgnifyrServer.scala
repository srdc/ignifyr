package io.ignifyr.server

import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.server.config.RedCapServiceConfig
import io.ignifyr.server.common.config.WebServerConfig
import io.ignifyr.server.endpoint.IgnifyrServerEndpoint
import io.onfhir.definitions.resource.fhir.FhirDefinitionsConfig

import scala.util.Try

object IgnifyrServer {
  def start(): Unit = {
    import io.ignifyr.engine.Execution.actorSystem

    val webServerConfig = new WebServerConfig(actorSystem.settings.config.getConfig("webserver"))
    val fhirDefinitionsConfig = new FhirDefinitionsConfig(actorSystem.settings.config.getConfig("fhir"))
    val redCapServiceConfig = Try(
      new RedCapServiceConfig(actorSystem.settings.config.getConfig("ignifyr-redcap"))
    ).toOption
    val endpoint =
      new IgnifyrServerEndpoint(IgnifyrConfig.engineConfig, webServerConfig, fhirDefinitionsConfig, redCapServiceConfig)

    IgnifyrHttpServer.start(endpoint.ignifyrRoute, webServerConfig)
  }
}
