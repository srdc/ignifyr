package io.ignifyr.engine.spi

import com.typesafe.config.Config

/**
 * Aggregate service-provider interface for an Ignifyr edition module.
 *
 * A single [[IgnifyrExtension]] is discovered per module through [[java.util.ServiceLoader]]
 * (one `META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension` entry per jar). Everything a
 * module contributes to the engine — source connectors, sinks, services, CLI commands, … — hangs
 * off this one descriptor, so moving a feature between the community and enterprise editions is a
 * plain folder move plus a reactor edit, with no engine code change.
 *
 * All contribution accessors default to empty; a module overrides only what it provides. Further
 * accessors (function libraries, streaming/scheduling capabilities, file-format handlers, extra
 * json4s hints) are introduced by later migration phases as they are needed — adding a defaulted
 * method here stays binary/source compatible with existing extensions.
 */
trait IgnifyrExtension {

  /**
   * Stable, unique identifier for this extension (e.g. "core", "connector-kafka").
   * Also selects the `ignifyr.extensions.<id>` HOCON subtree passed to [[initialize]].
   */
  def id: String

  /**
   * Called once during registry load with this extension's scoped configuration
   * (`ignifyr.extensions.<id>`, or an empty config if that block is absent).
   *
   * MUST NOT touch `IgnifyrConfig.sparkSession`: registry load is transitively triggered while the
   * shared SparkSession is being built (the session's config consults
   * [[ExtensionRegistry.sparkConfContributions]], which forces the extension list and hence every
   * `initialize`), so reading the session here would re-enter its lazy initialization and self-recurse.
   * Do Spark-dependent setup lazily, on first use, not in `initialize`.
   */
  def initialize(config: Config): Unit = ()

  /** Data-source readers this module contributes, keyed at registration by their binding class. */
  def sourceConnectors: Seq[SourceConnector] = Nil

  /** FHIR sink writers this module contributes, keyed at registration by their sink-settings class. */
  def sinkProviders: Seq[SinkProvider] = Nil

  /** Terminology-service providers, keyed at registration by their settings class. */
  def terminologyServiceProviders: Seq[TerminologyServiceProvider] = Nil

  /** Identity-service providers, keyed at registration by their settings class. */
  def identityServiceProviders: Seq[IdentityServiceProvider] = Nil

  /** CLI commands this module contributes, keyed at registration by name and aliases. */
  def cliCommands: Seq[CliCommandProvider] = Nil

  /** Source-failure descriptors this module contributes (e.g. a connector's client-specific errors). */
  def sourceFailureDescriptors: Seq[SourceFailureDescriptor] = Nil

  /** Source schema inferrers this module contributes, keyed at registration by their settings class. */
  def schemaInferrers: Seq[SourceSchemaInferrer] = Nil

  /** Streaming execution capability, if this module provides one. At most one may be installed. */
  def streamingProvider: Option[StreamingExecutionProvider] = None

  /** Scheduled (cron) execution capability, if this module provides one. At most one may be installed. */
  def schedulerProvider: Option[SchedulerProvider] = None

  /**
   * Extra Spark configuration entries this module contributes to the shared SparkSession — e.g. an
   * enterprise format that needs a Spark session extension or catalog (`spark.sql.extensions`,
   * `spark.sql.catalog.spark_catalog`). Merged into the session config when it is first built, below
   * the user-provided `spark { }` config (which still wins). Keeping this a pure accessor that never
   * touches the SparkSession avoids an initialization cycle.
   */
  def sparkConfContributions: Map[String, String] = Map.empty
}
