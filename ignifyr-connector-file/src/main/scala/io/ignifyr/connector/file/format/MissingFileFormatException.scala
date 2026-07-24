package io.ignifyr.connector.file.format

import io.ignifyr.engine.spi.MissingExtensionException

/**
 * Raised when a job references a file source content type whose format handler is not installed
 * (e.g. a `json` source without `ignifyr-format-json`).
 *
 * This is the file-connector analogue of the engine's `MissingConnectorException`, and — like it —
 * a [[MissingExtensionException]], so dispatch sites (`SourceHandler`) surface its actionable
 * "install the '…' module" message as-is instead of burying it in a generic read error.
 */
case class MissingFileFormatException(message: String) extends MissingExtensionException(message)
