package io.ignifyr.rxnorm

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/**
 * A local stand-in for the RxNorm REST API, bound on an ephemeral port for the lifetime of a suite.
 *
 * The client under test takes its root url as a constructor argument, so pointing it here is the whole
 * of the seam — no HTTP interception and no extra dependency (the stub uses the same akka-http the
 * client itself calls through). Testing against rxnav.nlm.nih.gov instead would make a green build
 * depend on a third party's uptime, and would give no way at all to exercise the not-found and
 * non-200 branches.
 *
 * A suite declares the canned bodies it needs in [[cannedResponses]], keyed by request path plus its
 * query parameters sorted by name; anything unlisted is answered with 404, which is what makes an
 * unexpected call visible instead of silent.
 */
trait RxNormApiStub extends BeforeAndAfterAll { this: Suite =>

  // The client keeps its own singleton ActorSystem; reusing it avoids standing up a second one.
  private implicit val actorSystem: ActorSystem = RxNormApiClient.actorSystem

  private var binding: Http.ServerBinding = _

  /** Root url to hand to the code under test. Valid between `beforeAll` and `afterAll`. */
  protected def rxNormRootUrl: String = s"http://localhost:${binding.localAddress.getPort}"

  /** Canned JSON bodies, keyed as `<path>` or `<path>?<name=value&...>` with the names sorted. */
  protected def cannedResponses: Map[String, String]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    binding = Await.result(
      Http()
        .newServerAt("localhost", 0)
        .bindSync { request =>
          cannedResponses.get(keyOf(request)) match {
            case Some(body) => HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, body))
            case None => HttpResponse(StatusCodes.NotFound)
          }
        },
      10.seconds
    )
  }

  override protected def afterAll(): Unit = {
    try Await.result(binding.unbind(), 10.seconds)
    finally super.afterAll()
  }

  /** The client builds some uris with an ordered query map and others by hand, so sort before matching. */
  private def keyOf(request: HttpRequest): String = {
    val path = request.uri.path.toString
    val parameters = request.uri.query().toMap.toSeq.sorted.map { case (name, value) => s"$name=$value" }
    if (parameters.isEmpty) path else s"$path?${parameters.mkString("&")}"
  }
}
