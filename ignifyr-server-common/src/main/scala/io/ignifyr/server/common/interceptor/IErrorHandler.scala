package io.ignifyr.server.common.interceptor

import com.typesafe.scalalogging.LazyLogging
import akka.http.scaladsl.server.{Directives, ExceptionHandler}
import io.onfhir.definitions.resource.model
import io.onfhir.definitions.resource.model.FhirDefinitionsError
import io.ignifyr.server.common.model.{BadRequest, InternalError, IgnifyrError, IgnifyrRestCall}

trait IErrorHandler extends LazyLogging {

  /**
   * Exception Handler object
   *
   * @return
   */
  def exceptionHandler(ignifyrRestCall: IgnifyrRestCall) =
    ExceptionHandler { case e: Exception =>
      Directives.complete {
        val response = exceptionToResponse(e)
        response.statusCode -> response.toString
      }
    }

  /**
   * Handling of exceptions by converting them to IgnifyrResponse
   *
   * @return
   */
  private def exceptionToResponse: PartialFunction[Exception, IgnifyrError] = {
    case e: IgnifyrError =>
      logger.error(s"Ignifyr error encountered: ${e.toString}", e)
      e
    // handle the errors coming from onFHIR Definitions Microservice
    case e: FhirDefinitionsError =>
      e match {
        case model.BadRequest(title, detail, cause) =>
          logger.error(s"Ignifyr error encountered: ${e.toString}", e)
          BadRequest(title, detail, cause)
        case _ =>
          logger.error("Unexpected internal error", e)
          InternalError(s"Unexpected internal error: ${e.getClass.getName}", e.getMessage, Some(e))
      }
    case e: Exception =>
      logger.error("Unexpected internal error", e)
      InternalError(s"Unexpected internal error: ${e.getClass.getName}", e.getMessage, Some(e))
  }
}
