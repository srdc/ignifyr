package io.ignifyr.server.common.model

import java.io.{PrintWriter, StringWriter}

/**
 * Any exception thrown by Ignifyr server
 */
abstract class IgnifyrError extends Exception {

  /**
   * HTTP status code to return when this error occurs
   */
  val statusCode: Int

  /**
   * Type of the error
   */
  val `type`: String = s"https://ignifyr.io/errors/${getClass.getSimpleName}"

  /**
   * Title of the error
   */
  val title: String

  /**
   * Details of the error
   */
  val detail: String

  /**
   * Inner exception
   */
  val cause: Option[Throwable] = None

  override def toString: String = {
    s"Status Code: $statusCode\n" +
      s"Type: ${`type`}\n" +
      s"Title: $title\n" +
      s"Detail: $detail\n" +
      s"Stack Trace: ${if (cause.isDefined) getStackTraceAsString(cause.get)}"
  }

  override def getMessage: String = s"$title $detail"

  private def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

}

case class BadRequest(title: String, detail: String, override val cause: Option[Throwable] = None)
    extends IgnifyrError {
  val statusCode = 400
}

case class AlreadyExists(title: String, detail: String) extends IgnifyrError {
  val statusCode = 409
}

case class ResourceNotFound(title: String, detail: String) extends IgnifyrError {
  val statusCode = 404
}

case class InternalError(title: String, detail: String, override val cause: Option[Throwable] = None)
    extends IgnifyrError {
  val statusCode = 500
}

case class UnsupportedMediaType(title: String, detail: String) extends IgnifyrError {
  val statusCode = 415
}

case class MethodForbidden(title: String, detail: String) extends IgnifyrError {
  val statusCode = 405
}

case class RequestTimeout(title: String, detail: String) extends IgnifyrError {
  val statusCode = 408
}
