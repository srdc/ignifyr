package io.ignifyr.engine.mapping.service

import com.typesafe.scalalogging.Logger
import io.onfhir.api.service.{IFhirIdentityService, IFhirTerminologyService}
import io.ignifyr.engine.model.{IdentityServiceSettings, TerminologyServiceSettings}
import io.ignifyr.engine.spi.{ExtensionRegistry, MissingServiceException}

/**
 * Factory for services that are used within mappings via FHIR Path functions.
 *
 * Kept as a stable facade over the extension registry: terminology/identity providers are
 * contributed by modules (local file terminology ships in the community engine; API-backed onFHIR
 * clients are an enterprise module) and looked up here by settings class.
 */
object IntegratedServiceFactory {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Create the terminology service registered for the given settings type.
   */
  def createTerminologyService(terminologyServiceSettings: TerminologyServiceSettings): IFhirTerminologyService = {
    try {
      ExtensionRegistry.terminologyServiceProviders
        .getOrElse(
          terminologyServiceSettings.getClass,
          throw MissingServiceException(
            s"No terminology service provider registered for '${terminologyServiceSettings.getClass.getSimpleName}'. " +
              "The module providing it is not installed."
          )
        )
        .create(terminologyServiceSettings)
    } catch {
      case t: Throwable =>
        logger.error("Failed to create terminology service", t)
        throw t
    }
  }

  /**
   * Create the identity service registered for the given settings type.
   */
  def createIdentityService(identityServiceSettings: IdentityServiceSettings): IFhirIdentityService = {
    ExtensionRegistry.identityServiceProviders
      .getOrElse(
        identityServiceSettings.getClass,
        throw MissingServiceException(
          s"No identity service provider registered for '${identityServiceSettings.getClass.getSimpleName}'. " +
            "The module providing it is not installed."
        )
      )
      .create(identityServiceSettings)
  }
}
