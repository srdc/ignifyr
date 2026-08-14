package io.ignifyr.sink.fhir

import io.onfhir.api.service.{IFhirIdentityService, IFhirTerminologyService}
import io.onfhir.client.{IdentityServiceClient, TerminologyServiceClient}
import io.ignifyr.engine.data.write.BaseSinkWriter
import io.ignifyr.engine.model.{
  FhirRepositorySinkSettings,
  IdentityServiceSettings,
  SinkSettings,
  TerminologyServiceSettings
}
import io.ignifyr.engine.spi._

import scala.concurrent.ExecutionContext

/**
 * Community FHIR-repository sink module: writes mapped FHIR resources to a FHIR server (the
 * product's flagship output) and — because the same server connection doubles as a terminology and
 * an identity service — also contributes the onfhir-client-backed terminology/identity providers
 * keyed by [[FhirRepositorySinkSettings]]. Discovered through ServiceLoader like every other
 * module, so the engine itself ships no concrete sinks.
 */
class FhirSinkExtension extends IgnifyrExtension {

  override val id: String = "sink-fhir"

  override def sinkProviders: Seq[SinkProvider] = Seq(
    new SinkProvider {
      override val id: String = "fhir-repository"
      override val settingsClass: Class[_ <: SinkSettings] = classOf[FhirRepositorySinkSettings]
      override def createWriter(sinkSettings: SinkSettings): BaseSinkWriter =
        new FhirRepositoryWriter(sinkSettings.asInstanceOf[FhirRepositorySinkSettings])
    }
  )

  override def terminologyServiceProviders: Seq[TerminologyServiceProvider] = Seq(
    new TerminologyServiceProvider {
      override val settingsClass: Class[_ <: TerminologyServiceSettings] = classOf[FhirRepositorySinkSettings]
      override def create(settings: TerminologyServiceSettings): IFhirTerminologyService = {
        implicit val ec: ExecutionContext = io.ignifyr.engine.Execution.actorSystem.dispatcher
        new TerminologyServiceClient(settings.asInstanceOf[FhirRepositorySinkSettings].createOnFhirClient)
      }
    }
  )

  override def identityServiceProviders: Seq[IdentityServiceProvider] = Seq(
    new IdentityServiceProvider {
      override val settingsClass: Class[_ <: IdentityServiceSettings] = classOf[FhirRepositorySinkSettings]
      override def create(settings: IdentityServiceSettings): IFhirIdentityService = {
        implicit val ec: ExecutionContext = io.ignifyr.engine.Execution.actorSystem.dispatcher
        new IdentityServiceClient(settings.asInstanceOf[FhirRepositorySinkSettings].createOnFhirClient)
      }
    }
  )
}
