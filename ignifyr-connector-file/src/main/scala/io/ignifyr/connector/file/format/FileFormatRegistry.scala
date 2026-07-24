package io.ignifyr.connector.file.format

import com.typesafe.scalalogging.Logger

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

/**
 * Registry of the installed file source [[FileSourceFormat]] handlers, discovered via
 * [[java.util.ServiceLoader]] over the classpath and keyed by content type. This is the file
 * connector's own extensibility seam, parallel to the engine's `ExtensionRegistry` but for the
 * sub-formats of the single `file` connector (the sink-format twin lives in `ignifyr-sink-file`'s
 * `FileSinkFormatRegistry`).
 *
 * The map is built once, on first access; a content type claimed by more than one handler is a
 * configuration error and fails fast, naming both owners. A content type with no handler is not an
 * error here — it surfaces as a [[MissingFileFormatException]] at the call site (so a job naming an
 * uninstalled format parses fine and fails only when it tries to read).
 */
object FileFormatRegistry {

  private val logger: Logger = Logger(this.getClass)

  lazy val sourceFormats: Map[String, FileSourceFormat] =
    indexUnique("file source format")(
      ServiceLoader
        .load(classOf[FileSourceFormat], classOf[FileSourceFormat].getClassLoader)
        .iterator()
        .asScala
        .toSeq
        .flatMap(format => format.contentTypes.map(_ -> format))
    )

  /** Resolves the source-format handler for a content type, or fails with an install hint. */
  def sourceFormat(contentType: String): FileSourceFormat =
    sourceFormats.getOrElse(
      contentType,
      throw MissingFileFormatException(FileFormatHints.describeSourceFormat(contentType))
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
