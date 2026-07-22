package io.ignifyr.server.common.spi

import io.onfhir.definitions.common.model.SchemaDefinition

import scala.concurrent.Future

/**
 * Persistence callback handed to [[IgnifyrServerExtension.schemaImportRoutes]]: an extension's
 * import route parses its format into [[SchemaDefinition]]s and saves them through this sink,
 * without depending on the server's repository layer.
 */
trait SchemaImportSink {

  /**
   * Persist the imported schema definitions for the given project.
   *
   * @return the saved schema definitions
   */
  def saveSchemas(projectId: String, schemas: Seq[SchemaDefinition]): Future[Seq[SchemaDefinition]]
}
