package io.ignifyr.sink.file.format

import com.typesafe.scalalogging.Logger

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

/**
 * Registry of the installed [[FileSinkFormat]] handlers, discovered via [[java.util.ServiceLoader]]
 * over the classpath and keyed by content type. This is the file sink's own extensibility seam,
 * parallel to the engine's `ExtensionRegistry` but for the sub-formats of the single `file` sink
 * (the source-format twin lives in `ignifyr-connector-file`'s `FileFormatRegistry`).
 *
 * The map is built once, on first access; a content type claimed by more than one handler is a
 * configuration error and fails fast, naming both owners. A content type with no handler is not an
 * error here — it surfaces as a [[MissingFileSinkFormatException]] at the call site (so a job naming
 * an uninstalled format parses fine and fails only when it tries to write).
 */
object FileSinkFormatRegistry {

  private val logger: Logger = Logger(this.getClass)

  lazy val sinkFormats: Map[String, FileSinkFormat] =
    indexUnique("file sink format")(
      ServiceLoader
        .load(classOf[FileSinkFormat], classOf[FileSinkFormat].getClassLoader)
        .iterator()
        .asScala
        .toSeq
        .flatMap(format => format.contentTypes.map(_ -> format))
    )

  /** Resolves the sink-format handler for a content type, or fails with an install hint. */
  def sinkFormat(contentType: String): FileSinkFormat =
    sinkFormats.getOrElse(
      contentType,
      throw MissingFileSinkFormatException(FileSinkFormatHints.describeSinkFormat(contentType))
    )

  private def indexUnique[V](what: String)(entries: Seq[(String, V)]): Map[String, V] = {
    val byKey = entries.groupBy(_._1).map { case (contentType, group) =>
      if (group.size > 1) {
        val owners = group.map(_._2.getClass.getName).mkString(", ")
        throw new IllegalStateException(
          s"Duplicate $what registration for content type '$contentType' from: $owners. " +
            "Each content type may be provided by exactly one installed handler."
        )
      }
      contentType -> group.head._2
    }
    logger.debug(s"Discovered ${byKey.size} $what handler(s): ${byKey.keys.toSeq.sorted.mkString(", ")}")
    byKey
  }
}
