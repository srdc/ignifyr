package io.ignifyr.connector.file.format

import com.typesafe.scalalogging.Logger
import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem}
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.{DataFrameWriter, Dataset, Row, SaveMode, SparkSession}

import java.net.URI

/**
 * Shared write helpers for the file sink formats. Owns the FHIR-aware "partition by resource type"
 * layout (both the local grouped write and the HDFS raw-text write) plus the configured
 * [[DataFrameWriter]] factory, so each [[FileSinkFormat]] — including the enterprise Delta writer in
 * a separate module — reuses the same machinery and only supplies the terminal, format-specific
 * write (`.text` / `.parquet` / `.format("delta").save`).
 */
object FileSinkSupport {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Creates a configured [[DataFrameWriter]] for a dataset from the sink settings (partition count,
   * append mode, extra write options).
   */
  def getWriter[T](dataset: Dataset[T], sinkSettings: FileSystemSinkSettings): DataFrameWriter[T] =
    dataset
      .coalesce(sinkSettings.numOfPartitions)
      .write
      .mode(SaveMode.Append)
      .options(sinkSettings.options)

  /**
   * Writes the mapped resources partitioned by FHIR resource type: one directory per resource type.
   * Shared by the ndjson/parquet/delta sink formats.
   *
   * @param singleColumnJson when true the per-resource-type frame is a single `mappedResourceJson`
   *                         string column (NDJSON); otherwise the JSON strings are parsed into a
   *                         multi-column frame (parquet/delta) with any missing partition columns added.
   * @param writeGroup       applies the terminal, format-specific write to the (already partition-configured)
   *                         writer and the resource-type output path.
   */
  def writePartitionedByResourceType(
      spark: SparkSession,
      df: Dataset[FhirMappingResult],
      sinkSettings: FileSystemSinkSettings,
      singleColumnJson: Boolean,
      writeGroup: (DataFrameWriter[Row], String) => Unit
  ): Unit = {
    // Check if the sink path is for HDFS
    if (sinkSettings.path.startsWith("hdfs://")) {
      writePartitionedToHdfs(df, sinkSettings)
    } else {
      // Group the DataFrame by resourceType to aggregate all resources of the same type.
      val groupedDFs =
        df.groupBy("resourceType").agg(collect_list("mappedFhirResource.mappedResource").as("resources"))
      // Iterate through each group (by resourceType) and write the data to separate folders.
      groupedDFs
        .collect()
        .foreach(rDf => {
          val resourceType = rDf.getAs[String]("resourceType")
          // Convert the mutable ArraySeq (default in Spark) to an immutable List
          val resourcesSeq = rDf.getAs[Seq[String]]("resources").toList
          if (resourceType == null) {
            // A mapped output without a `resourceType` discriminator (e.g. a flat/tabular mapping result)
            // cannot be routed to a per-type directory — skip it instead of writing a literal "null" folder.
            logger.warn(
              s"Skipping ${resourcesSeq.size} mapped result(s) without a 'resourceType' discriminator while " +
                s"writing partitioned by resource type to '${sinkSettings.path}'."
            )
          } else {
            writeResourceTypeGroup(spark, resourceType, resourcesSeq, sinkSettings, singleColumnJson, writeGroup)
          }
        })
    }
  }

  /** Writes one resource-type group of the local partition-by-resource-type layout. */
  private def writeResourceTypeGroup(
      spark: SparkSession,
      resourceType: String,
      resourcesSeq: List[String],
      sinkSettings: FileSystemSinkSettings,
      singleColumnJson: Boolean,
      writeGroup: (DataFrameWriter[Row], String) => Unit
  ): Unit = {
    import spark.implicits._
    val partitionColumns = sinkSettings.getPartitioningColumns(resourceType)

    val resourcesDF = if (singleColumnJson) {
      // Single-column frame of raw JSON strings (NDJSON output).
      resourcesSeq.toDF("mappedResourceJson")
    } else {
      // Parse the JSON strings into a multi-column frame (parquet/delta output).
      val resourcesDS = spark.createDataset(resourcesSeq)
      val parsed = spark.read.json(resourcesDS)
      if (partitionColumns.isEmpty) {
        parsed
      } else {
        // Some partition columns may not exist in the frame (e.g. nested fields like
        // `subject.reference`); add the missing ones so Spark can partition accordingly.
        val existingColumns = parsed.columns
        val filteredPartitionColumns =
          partitionColumns.filterNot(pc => existingColumns.exists(_.contentEquals(pc)))
        val allColumnsWithPartition =
          existingColumns.map(col) ++ filteredPartitionColumns.map(c => col(c).as(c))
        parsed.select(allColumnsWithPartition: _*)
      }
    }

    // Define the output path based on the resourceType, ensuring each type is in its own folder.
    val outputPath = s"${sinkSettings.path}/$resourceType"
    val writer = getWriter(resourcesDF, sinkSettings)
    // Apply partitioning if partition columns are specified
    val partitionedWriter =
      if (partitionColumns.nonEmpty) writer.partitionBy(partitionColumns: _*) else writer
    writeGroup(partitionedWriter, outputPath)
  }

  /**
   * HDFS variant of the partition-by-resource-type write: within each partition, group by resource
   * type and write the newline-joined mapped resources to `<path>/<resourceType>/<uuid>.txt` through
   * the Hadoop FileSystem API.
   */
  private def writePartitionedToHdfs(df: Dataset[FhirMappingResult], sinkSettings: FileSystemSinkSettings): Unit = {
    // Extract the scheme and authority from the sink path
    val uri = new URI(sinkSettings.path)
    val defaultFS = s"${uri.getScheme}://${uri.getAuthority}"

    df.foreachPartition((partition: Iterator[FhirMappingResult]) => {
      val fhirMappingResults: Seq[FhirMappingResult] = partition.toSeq
      if (fhirMappingResults.nonEmpty) {
        // Results without a `resourceType` discriminator cannot be routed to a per-type directory —
        // skip them instead of failing the whole partition on `resourceType.get`.
        val (routable, unrouted) = fhirMappingResults.partition(_.resourceType.isDefined)
        if (unrouted.nonEmpty) {
          logger.warn(
            s"Skipping ${unrouted.size} mapped result(s) without a 'resourceType' discriminator while " +
              s"writing partitioned by resource type to '${sinkSettings.path}'."
          )
        }
        routable.groupBy(_.resourceType.get).foreach { case (resourceType, fhirResources) =>
          logger.debug("Will write {} {} resources to HDFS.", fhirResources.length, resourceType)
          val data = fhirResources.map(_.mappedFhirResource.get.mappedResource.get).mkString("\n")

          val conf = new Configuration()
          conf.set("fs.defaultFS", defaultFS)
          val fs = FileSystem.get(conf)

          val dirPath = new org.apache.hadoop.fs.Path(s"${sinkSettings.path}/$resourceType")
          val uniqueFilePath =
            new org.apache.hadoop.fs.Path(dirPath, s"${java.util.UUID.randomUUID().toString}.txt")

          var outputStream: FSDataOutputStream = null
          try {
            if (!fs.exists(dirPath)) {
              fs.mkdirs(dirPath)
            }
            outputStream = fs.create(uniqueFilePath, true)
            outputStream.writeBytes(data)
            logger.info(s"Successfully wrote data to $uniqueFilePath")
          } catch {
            case e: Exception =>
              logger.error(s"Failed to write data to $uniqueFilePath: ${e.getMessage}", e)
              throw e
          } finally {
            if (outputStream != null) {
              try outputStream.close()
              catch {
                case e: Exception =>
                  logger.error(s"Failed to close output stream for $uniqueFilePath: ${e.getMessage}", e)
                  throw e
              }
            }
          }
        }
      }
    })
  }
}
