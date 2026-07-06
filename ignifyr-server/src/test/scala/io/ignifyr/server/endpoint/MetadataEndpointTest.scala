package io.ignifyr.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import io.ignifyr.server.BaseEndpointTest
import io.ignifyr.server.endpoint.MetadataEndpoint
import io.ignifyr.server.model.Metadata
import org.json4s.jackson.JsonMethods
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.ignifyr.engine.config.IgnifyrConfig

import java.io.File

class MetadataEndpointTest extends BaseEndpointTest {

  "Metadata endpoint" should {

    "return metadata information" in {
      Get(s"/${webServerConfig.baseUri}/${MetadataEndpoint.SEGMENT_METADATA}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val metadata = JsonMethods.parse(responseAs[String]).extract[Metadata]

        metadata.name shouldEqual "Ignifyr"
        metadata.description shouldEqual "Ignifyr is a tool for mapping data from various sources to FHIR resources."
        metadata.fhirDefinitionsVersion shouldEqual "R4"
        metadata.repositoryNames.mappings shouldEqual "mappings"
        metadata.repositoryNames.schemas shouldEqual "schemas"
        metadata.repositoryNames.contexts shouldEqual "mapping-contexts"
        metadata.repositoryNames.jobs shouldEqual "mapping-jobs"
        metadata.repositoryNames.terminologySystems shouldEqual "terminology-systems"
        metadata.archiving.erroneousRecordsFolder shouldEqual s"${IgnifyrConfig.engineConfig.contextPath}${File.separator}erroneous-records-folder"
        metadata.archiving.archiveFolder shouldEqual s"${IgnifyrConfig.engineConfig.contextPath}${File.separator}archive-folder"
        metadata.archiving.streamArchivingFrequency shouldEqual 5000
      }
    }

  }

}
