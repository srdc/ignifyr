package io.ignifyr.redcap

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.typesafe.config.Config
import io.ignifyr.server.common.model.IgnifyrRestCall
import io.ignifyr.server.common.spi.{IgnifyrServerExtension, SchemaImportSink}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Server-side REDCap contributions, discovered through ServiceLoader:
 * - the `/redcap` proxy endpoints for the sibling tofhir-redcap service, enabled only when the
 *   `ignifyr-redcap` HOCON block is present (mirroring the previous optional wiring in
 *   IgnifyrServer);
 * - the `/projects/{id}/schemas/redcap` data-dictionary import route, which needs no external
 *   service and is therefore always contributed;
 * - the version of the connected tofhir-redcap service for the server's `/metadata`.
 */
class RedCapServerExtension extends IgnifyrServerExtension {

  override val id: String = "redcap"

  private var redCapServiceConfig: Option[RedCapServiceConfig] = None

  override def initialize(rootConfig: Config): Unit =
    redCapServiceConfig = Try(new RedCapServiceConfig(rootConfig.getConfig("ignifyr-redcap"))).toOption

  override def rootRoutes: Seq[IgnifyrRestCall => Route] =
    redCapServiceConfig.toSeq.map(config => new RedCapEndpoint(config).route _)

  override def schemaImportRoutes(schemaImportSink: SchemaImportSink): Seq[IgnifyrRestCall => Route] =
    Seq(new RedCapSchemaImportEndpoint(schemaImportSink).route _)

  override def externalComponentVersion()(implicit ec: ExecutionContext): Future[Option[String]] =
    redCapServiceConfig match {
      case None => Future.successful(None)
      case Some(config) =>
        import io.ignifyr.engine.Execution.actorSystem
        val proxiedRequest = HttpRequest(
          method = HttpMethods.GET,
          uri = s"${config.endpoint}/metadata",
          headers = RawHeader("Content-Type", "application/json") :: Nil
        )
        Http()
          .singleRequest(proxiedRequest)
          .flatMap(resp => Unmarshal(resp.entity).to[String])
          .map(Some(_))
    }
}
