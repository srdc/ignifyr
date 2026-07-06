package io.ignifyr.server.common.model

import akka.http.scaladsl.model.{HttpMethod, RequestEntity, Uri}

import java.time.Instant

/**
 * Object representing a REST request wrapper for Ignifyr
 *
 * @param method    Http method
 * @param uri       Uri for the call
 * @param requestId Unique request identifier
 * @param requestEntity Request entity
 */
class IgnifyrRestCall(val method: HttpMethod, val uri: Uri, val requestId: String, val requestEntity: RequestEntity) {

  /**
   * Time of the request
   */
  val requestTime: Instant = Instant.now()

  /**
   * Project context for the call, if exists
   */
  var projectId: Option[String] = None

  /**
   * Terminology id for the call, if exists
   */
  var terminologyId: Option[String] = None
}
