package io.ignifyr.connector.file.format

/**
 * Raised when a job references a file content type whose format handler is not installed (e.g. a
 * `json` source without `ignifyr-format-json`, or a `delta` sink without `ignifyr-format-delta`).
 *
 * This is the file-connector analogue of the engine's `MissingConnectorException`/`MissingSinkException`.
 * It is a plain [[RuntimeException]] rather than a subtype of the engine's `MissingExtensionException`
 * because that hierarchy is `sealed` and cannot be extended from this downstream module; the message
 * still names the exact module to install.
 */
case class MissingFileFormatException(message: String) extends RuntimeException(message)
