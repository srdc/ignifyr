package io.ignifyr.sink.file

import io.ignifyr.FhirMappingResultFixtures
import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.data.write.{BaseSinkWriter, SinkHandler}
import io.ignifyr.engine.execution.log.ExecutionLogger
import io.ignifyr.engine.model._
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.jdk.CollectionConverters._

/**
 * The file sink coalesces its input to `numOfPartitions` (default 1) before writing. `coalesce` does not
 * shuffle, so on its own it would pull the upstream mapping stage into that many tasks, whatever
 * `mapping-jobs.numOfPartitions` says.
 *
 * What prevents that is the `df.cache()` in `SinkHandler.writeMappingResult`: the cache is built at the
 * mapping's own parallelism before the coalesced write reads it. For the plain ndjson layout that relies
 * on Spark's Adaptive Query Execution materializing the pending cache as its own stage; the parquet/csv
 * and partition-by-resource-type layouts run an earlier, fully parallel pass (schema inference, or the
 * per-type count) that builds it. Verified 2026-09-30: without the cache, ndjson maps in one task and
 * parquet/csv map every record twice; with AQE off, ndjson maps in one task.
 * The README's "Performance & Data-Volume Tuning" section documents the same contract.
 */
class FileSinkMappingParallelismTest extends AnyFlatSpec with Matchers {

  private val spark: SparkSession = IgnifyrConfig.sparkSession
  import spark.implicits._

  /** Stands in for `mapping-jobs.numOfPartitions`. */
  private val mappingPartitions = 4
  private val records = 400L

  private val execution: FhirMappingJobExecution = FhirMappingJobExecution(
    id = "parallelism-execution",
    job = FhirMappingJob(
      id = "parallelism-job",
      sourceSettings = Map.empty,
      sinkSettings = FileSystemSinkSettings("./out", SinkContentTypes.NDJSON),
      mappings = Seq.empty,
      dataProcessingSettings = DataProcessingSettings()
    )
  )
  // SinkHandler reports through ExecutionLogger, which needs the execution to have been started.
  ExecutionLogger.logExecutionStatus(execution, FhirMappingJobResult.STARTED)

  /**
   * Delegates to the real writer and snapshots the accumulator the moment the write returns. The write is
   * the first action `SinkHandler` runs, so the snapshot holds exactly the mapping work done for it.
   * Without the cache, `SinkHandler`'s later `count()`s recompute the mapping at full parallelism and
   * would mask a collapsed write if the accumulator were read at the end.
   */
  private class SnapshottingWriter(delegate: FileSystemWriter, ranIn: CollectionAccumulator[Int])
      extends BaseSinkWriter(FileSystemSinkSettings("./out", SinkContentTypes.NDJSON)) {
    var mappingRunsDuringWrite: Seq[Int] = Seq.empty
    override def write(
        spark: SparkSession,
        df: Dataset[FhirMappingResult],
        problemsAccumulator: CollectionAccumulator[FhirMappingResult]
    ): Unit = {
      delegate.write(spark, df, problemsAccumulator)
      mappingRunsDuringWrite = ranIn.value.asScala.map(_.intValue).toSeq
    }
    override def validate(): Unit = ()
  }

  /**
   * Runs a stand-in mapping stage (repartition, then flatMap, as `FhirMappingJobManager` and
   * `MappingTaskExecutor` do) through `SinkHandler` into a real `FileSystemWriter`, and asserts that,
   * while the sink was writing, every record was mapped exactly once and across all partitions.
   *
   * "Exactly once" matters as much as "all partitions": the parquet/csv writers infer a schema with
   * `spark.read.json` first (a separate, fully parallel scan), so without the cache the mapping runs
   * once in parallel for the inference and then a second time, collapsed, for the coalesced write.
   */
  private def assertMappedOnceInParallel(settings: String => FileSystemSinkSettings): Unit = {
    val fixture = FhirMappingResultFixtures.sampleFhirMappingResults(spark).collect()
    val ranIn = spark.sparkContext.collectionAccumulator[Int]
    val mapped: Dataset[FhirMappingResult] = spark
      .range(0, records)
      .repartition(mappingPartitions)
      .flatMap { i =>
        ranIn.add(TaskContext.getPartitionId())
        Seq(fixture((i % fixture.length).toInt))
      }
    val out = Files.createTempDirectory("ignifyr-sink-parallelism")
    val writer = new SnapshottingWriter(new FileSystemWriter(settings(out.toString)), ranIn)
    try SinkHandler.writeMappingResult(spark, execution, "task-1", mapped, writer)
    finally org.apache.commons.io.FileUtils.deleteDirectory(out.toFile)
    withClue("records mapped during the write: ") { writer.mappingRunsDuringWrite.size shouldBe records }
    withClue("partitions the mapping ran in: ") {
      writer.mappingRunsDuringWrite.distinct.size shouldBe mappingPartitions
    }
  }

  "The file sink" should "map in parallel, once, for ndjson with the default numOfPartitions = 1" in {
    assertMappedOnceInParallel(p => FileSystemSinkSettings(p, SinkContentTypes.NDJSON))
  }

  it should "map in parallel, once, for parquet with the default numOfPartitions = 1" in {
    assertMappedOnceInParallel(p => FileSystemSinkSettings(p, SinkContentTypes.PARQUET))
  }

  it should "map in parallel, once, for csv with the default numOfPartitions = 1" in {
    assertMappedOnceInParallel(p => FileSystemSinkSettings(p, SinkContentTypes.CSV))
  }

  it should "map in parallel, once, when partitioning by resource type" in {
    assertMappedOnceInParallel(p => FileSystemSinkSettings(p, SinkContentTypes.NDJSON, partitionByResourceType = true))
  }
}
