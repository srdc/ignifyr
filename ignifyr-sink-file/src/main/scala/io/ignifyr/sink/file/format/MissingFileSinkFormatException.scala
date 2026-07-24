package io.ignifyr.sink.file.format

import io.ignifyr.engine.spi.MissingExtensionException

/**
 * Raised when a job references a file sink content type whose format handler is not installed
 * (e.g. a `delta` sink without `ignifyr-format-delta`).
 *
 * This is the file-sink analogue of the engine's `MissingSinkException`, and — like it — a
 * [[MissingExtensionException]], so the actionable "install the '…' module" message surfaces as-is
 * instead of being buried in a generic write error.
 */
case class MissingFileSinkFormatException(message: String) extends MissingExtensionException(message)
