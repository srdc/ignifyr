package io.ignifyr.connector.file.format

/**
 * Static "which module provides this file format?" hints, used only to build actionable messages
 * when a source/sink format handler is missing — mirroring the engine's `ExtensionHints` for
 * connectors and sinks. Not dispatch logic: it maps a content-type string to the Maven coordinates
 * of the module that ships its handler, so the error can say exactly what to install. Content types
 * shipped by the community file connector itself (csv/tsv/parquet source, ndjson/csv/parquet sink)
 * fall through to a generic message, since they are present whenever the connector is.
 */
object FileFormatHints {

  private val sourceFormatModules: Map[String, String] = Map(
    "json" -> "com.pontegra.ignifyr:ignifyr-format-json",
    "ndjson" -> "com.pontegra.ignifyr:ignifyr-format-json"
  )

  private val sinkFormatModules: Map[String, String] = Map(
    "delta" -> "com.pontegra.ignifyr:ignifyr-format-delta"
  )

  def describeSourceFormat(contentType: String): String =
    describe("source", contentType, sourceFormatModules)

  def describeSinkFormat(contentType: String): String =
    describe("sink", contentType, sinkFormatModules)

  private def describe(kind: String, contentType: String, modules: Map[String, String]): String =
    modules.get(contentType) match {
      case Some(module) =>
        s"No file $kind format handler registered for content type '$contentType'. Install the '$module' module."
      case None =>
        s"No file $kind format handler registered for content type '$contentType'."
    }
}
