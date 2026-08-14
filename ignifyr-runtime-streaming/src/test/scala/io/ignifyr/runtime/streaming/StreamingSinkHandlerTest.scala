package io.ignifyr.runtime.streaming

import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.data.write.BaseSinkWriter
import io.ignifyr.engine.model.{
  DataProcessingSettings,
  FhirMappingJob,
  FhirMappingJobExecution,
  FhirMappingResult,
  FileSystemSourceSettings,
  SinkSettings
}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.apache.spark.util.CollectionAccumulator
import org.mockito.MockitoSugar._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * Sink writer that counts the chunks handed to it and throws on the first one.
 *
 * "Only the first chunk fails" is a property of the writer here rather than of a stateful mockito
 * argument matcher flipping a captured `var` — mockito re-evaluates matchers at times of its own
 * choosing, so making the *stub* carry the ordering is one less thing to reason about. Declared top
 * level, not as an inner class of the suite, so it carries no reference to the spec instance when Spark
 * closes over it.
 */
private class CountingSinkWriter extends BaseSinkWriter(new SinkSettings {}) {

  val chunks = new AtomicInteger(0)

  override def write(
      sparkSession: SparkSession,
      df: Dataset[FhirMappingResult],
      problemsAccumulator: CollectionAccumulator[FhirMappingResult]
  ): Unit =
    if (chunks.incrementAndGet() == 1) throw new RuntimeException("the first chunk fails on purpose")

  override def validate(): Unit = ()
}

class StreamingSinkHandlerTest extends AnyFlatSpec with BeforeAndAfterAll {

  val sparkSession: SparkSession = IgnifyrConfig.sparkSession

  /**
   * The streaming query below writes Spark offset/commit logs under a fixed checkpoint path
   * (jobId / mapping-task hash). Leftover logs from a previous run make the next run fail with
   * CONCURRENT_STREAM_LOG_UPDATE, so wipe the checkpoint directory around the suite — beforeAll
   * guards against a prior interrupted run, afterAll keeps the working tree clean.
   */
  private def clearCheckpointDirectory(): Unit =
    FileUtils.deleteQuietly(new File(IgnifyrConfig.sparkCheckpointDirectory))

  override def beforeAll(): Unit = clearCheckpointDirectory()

  override def afterAll(): Unit = clearCheckpointDirectory()

  "StreamingSinkHandler" should "continue processing subsequent chunks for streaming queries after a chunk throws an exception" in {
    val mockJob: FhirMappingJob = mock[FhirMappingJob]
    when(mockJob.id).thenReturn("jobId")
    when(mockJob.dataProcessingSettings).thenReturn(DataProcessingSettings.apply())
    // `asStream = true` matters: FhirMappingJobExecution derives isStreamingJob from it, and that flag is
    // what makes SinkHandler.logMappingJobResult take the streaming path. With the default (false) the
    // handler under test runs against an execution that claims to be a batch one, so after each write it
    // reaches ExecutionLogger.logExecutionResultForChunk, whose per-execution cache only a batch STARTED
    // log ever seeds — it throws NoSuchElementException, which this handler then swallows as a second
    // "chunk resulted in error". The assertions still held, but every chunk failed for a reason the suite
    // was not testing.
    when(mockJob.sourceSettings).thenReturn(
      Map("0" -> FileSystemSourceSettings.apply("name", "sourceUri", "dataFolderPath", asStream = true))
    )

    val execution: FhirMappingJobExecution = FhirMappingJobExecution("executionId", "projectId", mockJob)

    // A MemoryStream, not a `rate` source: it lets the test decide when a micro-batch exists, so the
    // assertions below are driven by what has actually been processed rather than by a wall-clock
    // window. A fixed window here was fragile in both directions -- Spark's streaming startup is what
    // dominates it, and it varies by machine (a 5s window left ~0.5s of margin on one developer box and
    // was far too short on a slower one).
    import sparkSession.implicits._
    implicit val sqlContext: SQLContext = sparkSession.sqlContext
    val source = MemoryStream[FhirMappingResult]

    val sinkWriter = new CountingSinkWriter
    val streamingQuery =
      StreamingSinkHandler.writeStream(sparkSession, execution, source.toDS(), sinkWriter, "someMappingTaskName")

    try {
      // First chunk: the writer throws. `processAllAvailable` returns only once the micro-batch is done,
      // so no waiting is guessed at.
      source.addData(mappingResult("first"))
      streamingQuery.processAllAvailable()
      assert(sinkWriter.chunks.get() == 1, "the first chunk should have reached the sink writer")

      // Second chunk: this is the actual contract -- the throwing chunk was swallowed inside
      // `foreachBatch`, so the query keeps consuming and the writer is handed the next one. Without this
      // the suite would still pass if the stream produced only the failing chunk and then nothing at all,
      // which is indistinguishable from swallow-and-continue from the query's side.
      source.addData(mappingResult("second"))
      streamingQuery.processAllAvailable()
      assert(sinkWriter.chunks.get() == 2, "the sink writer should have been handed a chunk after one threw")

      // The other half: the exception never escaped the micro-batch, so the query itself is unharmed.
      assert(streamingQuery.isActive, "the streaming query should have survived the throwing chunk")
      assert(streamingQuery.exception.isEmpty, "the streaming query should not have recorded a failure")
    } finally streamingQuery.stop()
  }

  private def mappingResult(source: String): FhirMappingResult =
    FhirMappingResult("jobId", "someMappingTaskName", Timestamp.from(Instant.now()), source)
}
