package io.ignifyr.runtime.streaming

import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.data.write.BaseSinkWriter
import io.ignifyr.engine.model.{
  DataProcessingSettings,
  FhirMappingJob,
  FhirMappingJobExecution,
  FhirMappingResult,
  FileSystemSourceSettings
}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import org.mockito.ArgumentMatchers
import org.mockito.MockitoSugar._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.sql.Timestamp
import java.time.Instant
import scala.language.postfixOps

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

  "StreamingSinkHandler" should "continue processing subsequent chunks for streaming queries after a chunk throws an exception" in { //
    // A mock
    val mockJob: FhirMappingJob = mock[FhirMappingJob]
    when(mockJob.id).thenReturn("jobId")
    when(mockJob.dataProcessingSettings).thenReturn(DataProcessingSettings.apply())
    when(mockJob.sourceSettings).thenReturn(
      Map("0" -> FileSystemSourceSettings.apply("name", "sourceUri", "dataFolderPath"))
    )

    val execution: FhirMappingJobExecution = FhirMappingJobExecution("executionId", "projectId", mockJob)

    // Create a Spark stream generating mapping results
    import sparkSession.implicits._
    val df = sparkSession.readStream
      .format("rate") // Generate timestamp and value tuples
      .option("rowsPerSecond", 1)
      .load()
      .coalesce(1)
      .map(_ => { // Map the generated rows to some dummy mapping result
        FhirMappingResult("someId", "someUrl", Timestamp.from(Instant.now()), "")
      })

    // Configure the mock writer such that it would throw an exception for the first chunk but not for the subsequent chunks
    var chunkCount = 0
    val mockWriter: BaseSinkWriter = mock[BaseSinkWriter]
    when(
      mockWriter.write(
        ArgumentMatchers.any(),
        ArgumentMatchers.argThat[Dataset[FhirMappingResult]]({ case _ =>
          if (chunkCount == 0) {
            chunkCount = chunkCount + 1
            true
          } else {
            false
          }
        }),
        ArgumentMatchers.any()
      )
    ).thenThrow(new Exception())

    // Start streaming
    val streamingQuery =
      StreamingSinkHandler.writeStream(sparkSession, execution, df, mockWriter, "someMappingTaskName")

    // Wait for data generation for 5 seconds and then terminate the query
    streamingQuery.awaitTermination(5000)
    streamingQuery.stop()
  }
}
