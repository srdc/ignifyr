package io.ignifyr.engine.spi

/**
 * Base type for "a job needs a capability that no installed module provides" failures. These are
 * raised lazily, at the point a job actually exercises the missing feature — the job JSON itself
 * always parses, because every settings/model class stays in the engine.
 *
 * Not sealed: downstream plugin modules define their own missing-feature failures under this type
 * (e.g. the file connector's `MissingFileFormatException` for an uninstalled file format), so that
 * dispatch sites can surface the actionable "install …" message uniformly (see `SourceHandler`).
 */
abstract class MissingExtensionException(message: String) extends RuntimeException(message)

/** No [[SourceConnector]] is registered for a job's source binding type. */
case class MissingConnectorException(message: String) extends MissingExtensionException(message)

/** No [[SinkProvider]] is registered for a job's sink-settings type. */
case class MissingSinkException(message: String) extends MissingExtensionException(message)

/** No terminology/identity [[TerminologyServiceProvider]]/[[IdentityServiceProvider]] is registered. */
case class MissingServiceException(message: String) extends MissingExtensionException(message)

/**
 * A required runtime capability (streaming execution, scheduling) has no provider installed.
 * Introduced now so the batch-only community engine can fail clearly once those seams are carved
 * out into their own modules.
 */
case class MissingCapabilityException(message: String) extends MissingExtensionException(message)
