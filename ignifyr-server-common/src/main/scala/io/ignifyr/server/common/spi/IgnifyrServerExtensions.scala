package io.ignifyr.server.common.spi

import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

/**
 * Discovers the installed [[IgnifyrServerExtension]] modules through [[java.util.ServiceLoader]]
 * over the classpath (the shaded server jar merges `META-INF/services` through the shade
 * `ServicesResourceTransformer`). Called once by the server's composition root; the resulting
 * sequence is passed to the endpoints that consult extension contributions.
 */
object IgnifyrServerExtensions {

  private val logger: Logger = Logger(this.getClass)

  /** Load all server extensions, ordered by id, each initialized with the server's root config. */
  def load(rootConfig: Config): Seq[IgnifyrServerExtension] = {
    val loaded =
      ServiceLoader
        .load(classOf[IgnifyrServerExtension], classOf[IgnifyrServerExtension].getClassLoader)
        .iterator()
        .asScala
        .toSeq
        .sortBy(_.id)
    loaded.foreach(_.initialize(rootConfig))
    logger.info(s"Loaded ${loaded.size} Ignifyr server extension(s): ${loaded.map(_.id).mkString(", ")}")
    loaded
  }
}
