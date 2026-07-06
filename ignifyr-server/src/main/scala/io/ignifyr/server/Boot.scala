package io.ignifyr.server

import com.typesafe.scalalogging.Logger
import io.ignifyr.common.app.AppVersion

object Boot extends App {
  val logger: Logger = Logger(this.getClass)
  logger.info(s"Starting Ignifyr version: ${AppVersion.getVersion}")

  IgnifyrServer.start()
}
