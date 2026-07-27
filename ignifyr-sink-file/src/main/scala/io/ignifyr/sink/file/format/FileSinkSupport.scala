package io.ignifyr.sink.file.format

import com.typesafe.scalalogging.Logger
import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrameWriter, Dataset, Row, SaveMode, SparkSession}

/**
 * Shared write helpers for the file sink formats. Owns the FHIR-aware "partition by resource type"
 * layout plus the configured [[DataFrameWriter]] factory, so each [[FileSinkFormat]] — including the
 * enterprise Delta writer in a separate module — reuses the same machinery and only supplies the
 * terminal, format-specific write (`.text` / `.parquet` / `.format("delta").save`).
 *
 * Paths are handed to Spark's [[DataFrameWriter]] as given, so any Hadoop-supported scheme
 * (`hdfs://`, `s3a://`, a plain local path, …) works and is written in the requested format. There is
 * deliberately no per-scheme branch here: an earlier `hdfs://` special case wrote raw text regardless
 * of the content type, silently turning parquet/delta output into `.txt` files.
 */
object FileSinkSupport {

  private val logger: Logger = Logger(this.getClass)

  /** The mapped output payload — a JSON string — inside a [[FhirMappingResult]]. */
  private val MappedResourceColumn = "mappedFhirResource.mappedResource"

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
   * Each resource type is written from its own filtered [[Dataset]], so the mapped payloads stay
   * distributed and are never collected to the driver.
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
    import spark.implicits._

    // Results carrying a mapped payload, narrowed to the two columns the write needs. A result without
    // a payload is nothing to write, so it is dropped here as it always has been.
    // It is scanned once per resource type below, so materialize it first: what sits upstream is a whole
    // mapping pipeline, not a cheap file scan. Persisting this narrowed frame leaves any cache the
    // caller holds on `df` untouched.
    val payloads = df
      .filter(col(MappedResourceColumn).isNotNull)
      .select(col("resourceType"), col(MappedResourceColumn).as("mappedResourceJson"))
    payloads.persist()
    try {
      // One small aggregate gives both the resource types present and how many results cannot be routed;
      // only these per-type counts reach the driver, never the payloads themselves.
      val (unroutable, routableTypes) = payloads.groupBy("resourceType").count().collect().partition(_.isNullAt(0))
      unroutable.foreach(row =>
        // A mapped output without a `resourceType` discriminator (e.g. a flat/tabular mapping result)
        // cannot be routed to a per-type directory — skip it instead of writing a literal "null" folder.
        logger.warn(
          s"Skipping ${row.getLong(1)} mapped result(s) without a 'resourceType' discriminator while " +
            s"writing partitioned by resource type to '${sinkSettings.path}'."
        )
      )
      routableTypes.map(_.getString(0)).sorted.foreach { resourceType =>
        val resourceJson =
          payloads.filter(col("resourceType") === resourceType).select("mappedResourceJson").as[String]
        writeResourceTypeGroup(spark, resourceType, resourceJson, sinkSettings, singleColumnJson, writeGroup)
      }
    } finally
      payloads.unpersist()
  }

  /** Writes one resource-type group of the partition-by-resource-type layout. */
  private def writeResourceTypeGroup(
      spark: SparkSession,
      resourceType: String,
      resourceJson: Dataset[String],
      sinkSettings: FileSystemSinkSettings,
      singleColumnJson: Boolean,
      writeGroup: (DataFrameWriter[Row], String) => Unit
  ): Unit = {
    val partitionColumns = sinkSettings.getPartitioningColumns(resourceType)

    val resourcesDF = if (singleColumnJson) {
      // Single-column frame of raw JSON strings (NDJSON output).
      resourceJson.toDF("mappedResourceJson")
    } else {
      // Parse the JSON strings into a multi-column frame (parquet/delta output).
      val parsed = spark.read.json(resourceJson)
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
}
