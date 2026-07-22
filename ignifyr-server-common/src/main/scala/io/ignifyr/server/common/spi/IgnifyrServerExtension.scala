package io.ignifyr.server.common.spi

import akka.http.scaladsl.server.Route
import com.typesafe.config.Config
import io.ignifyr.server.common.model.IgnifyrRestCall

import scala.concurrent.{ExecutionContext, Future}

/**
 * Aggregate service-provider interface for a module contributing to the Ignifyr web server — the
 * server-side counterpart of the engine's `IgnifyrExtension`. One [[IgnifyrServerExtension]] is
 * discovered per module through [[java.util.ServiceLoader]] (one
 * `META-INF/services/io.ignifyr.server.common.spi.IgnifyrServerExtension` entry per jar), so
 * moving a server feature between editions is a plain folder move plus a reactor edit, with no
 * server code change.
 *
 * All contribution accessors default to empty; a module overrides only what it provides. The
 * route accessors are consulted once, while the server assembles its route tree — an extension
 * decides there (typically from configuration read in [[initialize]]) whether to contribute.
 */
trait IgnifyrServerExtension {

  /** Stable, unique identifier for this extension (e.g. "redcap"). */
  def id: String

  /**
   * Called once during discovery with the server's root configuration. Extensions read their own
   * top-level blocks from it (e.g. `ignifyr-redcap`) and may disable their contributions when the
   * block is absent.
   */
  def initialize(rootConfig: Config): Unit = ()

  /** Endpoint routes mounted at the API root, beside the server's built-in endpoints. */
  def rootRoutes: Seq[IgnifyrRestCall => Route] = Nil

  /**
   * Routes mounted under `/projects/{projectId}/schemas` for module-specific schema imports
   * (e.g. a data-dictionary format). Imported schemas are persisted through the server-provided
   * [[SchemaImportSink]].
   */
  def schemaImportRoutes(schemaImportSink: SchemaImportSink): Seq[IgnifyrRestCall => Route] = Nil

  /**
   * Version of the external component this extension integrates with (e.g. the tofhir-redcap
   * service), reported by the server's `/metadata`. None when unknown or not applicable.
   */
  def externalComponentVersion()(implicit ec: ExecutionContext): Future[Option[String]] =
    Future.successful(None)
}
