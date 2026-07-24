package io.ignifyr.sink.file.format

/**
 * Static "which module provides this file sink format?" hints, used only to build actionable
 * messages when a sink-format handler is missing — mirroring the engine's `ExtensionHints` for
 * connectors and sinks. Not dispatch logic: it maps a content-type string to the Maven coordinates
 * of the module that ships its handler, so the error can say exactly what to install. Content types
 * shipped by this module itself (ndjson/csv/parquet) fall through to a generic message, since they
 * are present whenever the sink is.
 */
object FileSinkFormatHints {

  private val sinkFormatModules: Map[String, String] = Map(
    "delta" -> "com.pontegra.ignifyr:ignifyr-format-delta"
  )

  def describeSinkFormat(contentType: String): String =
    sinkFormatModules.get(contentType) match {
      case Some(module) =>
        s"No file sink format handler registered for content type '$contentType'. Install the '$module' module."
      case None =>
        s"No file sink format handler registered for content type '$contentType'."
    }
}
