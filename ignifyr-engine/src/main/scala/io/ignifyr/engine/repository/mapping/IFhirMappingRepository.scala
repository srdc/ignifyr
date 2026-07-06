package io.ignifyr.engine.repository.mapping

import io.ignifyr.engine.model.FhirMapping
import io.ignifyr.engine.repository.ICachedRepository

trait IFhirMappingRepository extends ICachedRepository {

  /**
   * Return the Fhir mapping definition by given url
   *
   * @param mappingUrl Fhir mapping url
   * @return
   */
  def getFhirMappingByUrl(mappingUrl: String): FhirMapping

}
