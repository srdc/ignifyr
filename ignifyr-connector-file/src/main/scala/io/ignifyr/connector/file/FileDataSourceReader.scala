package io.ignifyr.connector.file

import com.typesafe.scalalogging.Logger
import io.ignifyr.connector.file.format.{FileFormatRegistry, FileSourceReadContext}
import io.ignifyr.engine.data.read.BaseDataSourceReader
import io.ignifyr.engine.model.{FileSystemSource, FileSystemSourceSettings}
import io.ignifyr.engine.util.FileUtils
import org.apache.spark.sql.functions.{input_file_name, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.net.URI
import java.time.LocalDateTime
import scala.collection.mutable

/**
 * Reader from the file system. Resolves the path and handles the cross-cutting concerns (streaming
 * directory validation, the streaming filename-logging column, and the `distinct` option), then
 * delegates the per-content-type read to the [[FileFormatRegistry]] — so the set of supported file
 * formats is itself pluggable (e.g. JSON/NDVSON source via the enterprise `ignifyr-format-json`).
 *
 * @param spark Spark session
 */
class FileDataSourceReader(spark: SparkSession)
    extends BaseDataSourceReader[FileSystemSource, FileSystemSourceSettings] {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Read the source data from file system.
   *
   * @throws IllegalArgumentException      If the path is not a directory for streaming jobs.
   * @throws io.ignifyr.connector.file.format.MissingFileFormatException If no format handler is registered for the content type.
   */
  override def read(
      mappingSourceBinding: FileSystemSource,
      mappingJobSourceSettings: FileSystemSourceSettings,
      schema: Option[StructType],
      timeRange: Option[(LocalDateTime, LocalDateTime)] = Option.empty,
      jobId: Option[String] = Option.empty
  ): DataFrame = {
    // check whether it is a zip file
    val isZipFile = mappingSourceBinding.path.endsWith(".zip")
    // determine the final path
    // if it is a Hadoop path (starts with "hdfs://"), construct the URI directly without adding the context path
    val finalPath = if (mappingJobSourceSettings.dataFolderPath.startsWith("hdfs://")) {
      new URI(
        s"${mappingJobSourceSettings.dataFolderPath.stripSuffix("/")}/${mappingSourceBinding.path.stripPrefix("/")}"
      ).toString
    } else {
      FileUtils.getPath(mappingJobSourceSettings.dataFolderPath, mappingSourceBinding.path).toAbsolutePath.toString
    }
    // validate whether the provided path is a directory when streaming is enabled in the source settings
    if (mappingJobSourceSettings.asStream && !new File(finalPath).isDirectory) {
      throw new IllegalArgumentException(
        s"$finalPath is not a directory. For streaming job, you should provide a directory."
      )
    }

    val isDistinct = mappingSourceBinding.options.get("distinct").contains("true")

    // keeps the names of processed files by Spark
    val processedFiles: mutable.HashSet[String] = mutable.HashSet.empty

    // Resolve the format handler for the content type and read the raw frame.
    val format = FileFormatRegistry.sourceFormat(mappingSourceBinding.contentType)
    val rawDf = format.read(
      FileSourceReadContext(spark, finalPath, isZipFile, mappingSourceBinding, mappingJobSourceSettings, schema)
    )

    // For streaming reads, add a dummy column called 'filename' via a udf, to print a log when data
    // reading is started for a file.
    val resultDf =
      if (mappingJobSourceSettings.asStream)
        rawDf.withColumn(
          "filename",
          logStartOfDataReading(processedFiles, logger = logger, jobId = jobId)(input_file_name)
        )
      else rawDf

    if (isDistinct) resultDf.distinct() else resultDf
  }

  /**
   * A user-defined function i.e. udf to print a log when data reading is started for a file. udf takes the
   * name of input file being read and returns it after logging a message to indicate that data reading is started and
   * it may take a while. It makes use of the given processedFiles set to decide whether to print a log. If it does not
   * contain the file name i.e. Spark just started to process it, the log is printed.
   *
   * @param processedFiles The set of file names processed by Spark
   * @param logger         Logger instance
   * @param jobId          The identifier of mapping job which executes the mapping
   * @return a user-defined function to print a log when data reading is started for a file
   * */
  private def logStartOfDataReading(processedFiles: mutable.HashSet[String], logger: Logger, jobId: Option[String]) =
    udf((fileName: String) => {
      // if the file is not processed yet, print the log and add it to the processed files set
      if (!processedFiles.contains(fileName)) {
        logger.info(s"Reading data from $fileName for the mapping job ${jobId.getOrElse("")}. This may take a while...")
        // add it to the set
        processedFiles.add(fileName)
      }
      fileName
    })
}
