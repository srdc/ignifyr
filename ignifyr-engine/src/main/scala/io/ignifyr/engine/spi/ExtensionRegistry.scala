package io.ignifyr.engine.spi

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Central, lazily-built registry of everything the installed [[IgnifyrExtension]] modules
 * contribute. Discovery is via [[java.util.ServiceLoader]] over the classpath (the shaded jars
 * already merge `META-INF/services` through the shade `ServicesResourceTransformer`).
 *
 * Lookup maps are keyed by model class (or command token) and built once, on first access. A key
 * claimed by more than one extension is a configuration error and fails fast, naming both owners.
 * Absence is not an error here — it surfaces as a `Missing*Exception` at the call site, so a job
 * referencing an uninstalled feature parses fine and fails only when it tries to use it.
 */
object ExtensionRegistry {

  private val logger: Logger = Logger(this.getClass)

  /** All discovered extensions, ordered by id, each initialized with its scoped config subtree. */
  lazy val extensions: Seq[IgnifyrExtension] = {
    val rootConfig: Config =
      Try(io.ignifyr.engine.Execution.actorSystem.settings.config).getOrElse(ConfigFactory.load())
    val loaded =
      ServiceLoader
        .load(classOf[IgnifyrExtension], classOf[IgnifyrExtension].getClassLoader)
        .iterator()
        .asScala
        .toSeq
        .sortBy(_.id)
    loaded.foreach { ext =>
      val path = s"ignifyr.extensions.${ext.id}"
      val scoped = if (rootConfig.hasPath(path)) rootConfig.getConfig(path) else ConfigFactory.empty()
      ext.initialize(scoped)
    }
    logger.info(s"Loaded ${loaded.size} Ignifyr extension(s): ${loaded.map(_.id).mkString(", ")}")
    loaded
  }

  lazy val sourceConnectors: Map[Class[_], SourceConnector] =
    indexUnique("source connector")(
      extensions.flatMap(e => e.sourceConnectors.map(c => (e.id, c.bindingClass: Class[_], c)))
    )

  lazy val sinkProviders: Map[Class[_], SinkProvider] =
    indexUnique("sink provider")(
      extensions.flatMap(e => e.sinkProviders.map(p => (e.id, p.settingsClass: Class[_], p)))
    )

  lazy val terminologyServiceProviders: Map[Class[_], TerminologyServiceProvider] =
    indexUnique("terminology service provider")(
      extensions.flatMap(e => e.terminologyServiceProviders.map(p => (e.id, p.settingsClass: Class[_], p)))
    )

  lazy val identityServiceProviders: Map[Class[_], IdentityServiceProvider] =
    indexUnique("identity service provider")(
      extensions.flatMap(e => e.identityServiceProviders.map(p => (e.id, p.settingsClass: Class[_], p)))
    )

  lazy val cliCommands: Map[String, CliCommandProvider] =
    indexUnique("CLI command")(
      extensions.flatMap(e => e.cliCommands.flatMap(cmd => (cmd.name +: cmd.aliases).map(token => (e.id, token, cmd))))
    )

  /**
   * Force every registry to materialize — so a duplicate-registration error surfaces at engine
   * startup rather than in the middle of a job — and log what was loaded.
   */
  def init(): Unit = {
    sourceConnectors
    sinkProviders
    terminologyServiceProviders
    identityServiceProviders
    cliCommands
    ()
  }

  /**
   * Index `(ownerExtensionId, key, value)` triples into a `key -> value` map, failing with both
   * owner ids if any key is claimed twice.
   */
  private def indexUnique[K, V](what: String)(entries: Seq[(String, K, V)]): Map[K, V] =
    entries.groupBy(_._2).map { case (key, group) =>
      if (group.size > 1) {
        val owners = group.map(_._1).mkString(", ")
        throw new IllegalStateException(
          s"Duplicate $what registration for key '$key' from extensions: $owners. " +
            "Each key may be provided by exactly one installed module."
        )
      }
      key -> group.head._3
    }
}
