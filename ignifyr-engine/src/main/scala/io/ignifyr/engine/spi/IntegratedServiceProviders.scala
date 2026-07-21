package io.ignifyr.engine.spi

import io.onfhir.api.service.{IFhirIdentityService, IFhirTerminologyService}
import io.ignifyr.engine.model.{IdentityServiceSettings, TerminologyServiceSettings}

/**
 * Contributes a terminology service for one settings type (e.g. a local file-based service, or an
 * API-backed onFHIR terminology client). Registered/looked up by [[settingsClass]].
 */
trait TerminologyServiceProvider {

  /** Terminology-settings model class this provider handles (the registry lookup key). */
  def settingsClass: Class[_ <: TerminologyServiceSettings]

  /** Build the terminology service for the given settings. */
  def create(settings: TerminologyServiceSettings): IFhirTerminologyService
}

/**
 * Contributes an identity service for one settings type. Registered/looked up by [[settingsClass]].
 */
trait IdentityServiceProvider {

  /** Identity-settings model class this provider handles (the registry lookup key). */
  def settingsClass: Class[_ <: IdentityServiceSettings]

  /** Build the identity service for the given settings. */
  def create(settings: IdentityServiceSettings): IFhirIdentityService
}
