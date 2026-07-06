package io.ignifyr.engine.mapping

import io.onfhir.api.Resource
import io.ignifyr.engine.model.FhirInteraction
import io.ignifyr.engine.model.exception.FhirMappingException
import org.json4s.JsonAST.{JObject, JValue}

import scala.concurrent.Future

/**
 * Mapping service for a specific mapping definition (FhirMapping)
 */
trait IFhirMappingService extends Serializable {

  /**
   * Map the given source into one or more FHIR resources based on the underlying mapping definition for this service
   *
   * @param source
   * @return Mapping expression identifier, Mapped resources, optional FHIR interaction for persistence details
   */
  @throws[FhirMappingException]
  def mapToFhir(
      source: JObject,
      contextVariables: Map[String, JValue] = Map.empty
  ): Future[Seq[(String, Seq[Resource], Option[FhirInteraction])]]

}
