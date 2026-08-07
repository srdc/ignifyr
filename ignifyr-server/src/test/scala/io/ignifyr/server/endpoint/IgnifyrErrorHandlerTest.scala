package io.ignifyr.server.endpoint

import akka.http.scaladsl.model.{HttpEntity, HttpMethods, StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.onfhir.definitions.resource.model
import io.ignifyr.server.common.interceptor.IErrorHandler
import io.ignifyr.server.common.model.{IgnifyrRestCall, RequestTimeout, ResourceNotFound}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * The exception half of the server's error handling. `IgnifyrRejectionHandlerTest` covers *rejections* —
 * a different Akka mechanism, reached when no route matches or a directive refuses the request. What is
 * covered here is what happens when a route **throws**: every endpoint is wrapped in this handler, so it
 * is the last thing standing between an unexpected failure and an empty 500 with a leaked stack trace.
 *
 * The suite drives a minimal route rather than the real API because no endpoint can be made to throw an
 * arbitrary exception on demand.
 */
class IgnifyrErrorHandlerTest extends AnyWordSpec with Matchers with ScalatestRouteTest with IErrorHandler {

  private val restCall =
    new IgnifyrRestCall(HttpMethods.GET, Uri("/ignifyr/boom"), "test-request", HttpEntity.Empty)

  /** A route whose only job is to throw whatever the test hands it. */
  private def routeThrowing(exception: Exception): Route =
    handleExceptions(exceptionHandler(restCall)) {
      path("boom") {
        get {
          complete {
            throw exception
          }
        }
      }
    }

  "The error handler" should {

    "answer an unexpected exception with 500 and name the exception type" in {
      Get("/boom") ~> routeThrowing(new IllegalStateException("spark session is gone")) ~> check {
        status shouldEqual StatusCodes.InternalServerError
        val body = responseAs[String]
        body should include("Type: https://ignifyr.io/errors/InternalError")
        body should include("java.lang.IllegalStateException")
        body should include("spark session is gone")
      }
    }

    // An IgnifyrError already carries its own status; the handler must pass it through untouched rather
    // than flattening everything to 500.
    "pass an IgnifyrError through with its own status" in {
      Get("/boom") ~> routeThrowing(ResourceNotFound("Job not found", "No job with id x")) ~> check {
        status shouldEqual StatusCodes.NotFound
        val body = responseAs[String]
        body should include("Type: https://ignifyr.io/errors/ResourceNotFound")
        body should include("No job with id x")
      }
    }

    "pass a request timeout through as 408" in {
      Get("/boom") ~> routeThrowing(RequestTimeout("Timed out", "The FHIR server did not answer")) ~> check {
        status shouldEqual StatusCodes.RequestTimeout
        responseAs[String] should include("Type: https://ignifyr.io/errors/RequestTimeout")
      }
    }

    // onFHIR's definitions layer raises its own error type; a bad request from it must stay a 400 rather
    // than being reported to the user as an internal failure.
    "translate an onFHIR bad request into a 400" in {
      Get("/boom") ~> routeThrowing(model.BadRequest("Invalid profile", "Profile url is malformed", None)) ~> check {
        status shouldEqual StatusCodes.BadRequest
        val body = responseAs[String]
        body should include("Type: https://ignifyr.io/errors/BadRequest")
        body should include("Profile url is malformed")
      }
    }

    "answer with 500 when an exception carries no message" in {
      Get("/boom") ~> routeThrowing(new RuntimeException()) ~> check {
        status shouldEqual StatusCodes.InternalServerError
        responseAs[String] should include("Type: https://ignifyr.io/errors/InternalError")
      }
    }
  }
}
