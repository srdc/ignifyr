package io.ignifyr.redcap

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.ignifyr.engine.Execution.actorSystem.dispatcher
import io.ignifyr.engine.util.CsvUtil
import io.ignifyr.redcap.RedCapSchemaImportEndpoint.{ATTACHMENT, SEGMENT_REDCAP}
import io.ignifyr.server.common.model.IgnifyrRestCall
import io.ignifyr.server.common.spi.SchemaImportSink
import io.onfhir.definitions.common.model.Json4sSupport._
import io.onfhir.definitions.common.model.SchemaDefinition

/**
 * The `/projects/{projectId}/schemas/redcap` route: imports a REDCap data dictionary file and
 * creates a schema for each form defined in it. Contributed to the server's schemas endpoint via
 * [[io.ignifyr.server.common.spi.IgnifyrServerExtension.schemaImportRoutes]]; the schemas are
 * persisted through the server-provided [[SchemaImportSink]].
 */
class RedCapSchemaImportEndpoint(schemaImportSink: SchemaImportSink) extends LazyLogging {

  def route(request: IgnifyrRestCall): Route = {
    val projectId: String = request.projectId.get
    pathPrefix(SEGMENT_REDCAP) {
      importREDCapDataDictionary(projectId)
    }
  }

  /**
   * Route to import a REDCap data dictionary file which will be used to create schemas.
   * */
  private def importREDCapDataDictionary(projectId: String): Route = {
    post {
      fileUpload(ATTACHMENT) { case (_, byteSource) =>
        parameters("rootUrl", "recordIdField") { (rootUrl, recordIdField) =>
          complete {
            CsvUtil.readFromCSVSource(byteSource).flatMap { rows =>
              // extract schema definitions from the data dictionary rows and save them
              val definitions: Seq[SchemaDefinition] =
                RedCapUtil.extractSchemasAsSchemaDefinitions(rows, rootUrl, recordIdField)
              schemaImportSink.saveSchemas(projectId, definitions)
            }
          }
        }
      }
    }
  }
}

object RedCapSchemaImportEndpoint {
  val SEGMENT_REDCAP = "redcap"
  private val ATTACHMENT = "attachment"
}
