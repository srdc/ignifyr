package io.ignifyr.runtime.streaming

import com.typesafe.scalalogging.Logger
import io.ignifyr.engine.data.write.{BaseSinkWriter, SinkHandler}
import io.ignifyr.engine.model.{FhirMappingJobExecution, FhirMappingResult}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Streaming sink: wraps the engine's batch [[SinkHandler.writeMappingResult]] in a Spark
 * structured-streaming `foreachBatch`, so each micro-batch is written exactly as a batch chunk.
 * Moved out of the engine as part of the streaming capability.
 */
object StreamingSinkHandler {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Writes streaming FHIR mapping results to the given writer via per-micro-batch delegation to the
   * engine's batch writer. Uses a per-job, per-mapping-task checkpoint directory so distinct streams
   * do not share offsets.
   */
  def writeStream(
      spark: SparkSession,
      mappingJobExecution: FhirMappingJobExecution,
      df: Dataset[FhirMappingResult],
      resourceWriter: BaseSinkWriter,
      mappingTaskName: String
  ): StreamingQuery = {
    val datasetWrite = (dataset: Dataset[FhirMappingResult], _: Long) =>
      try {
        SinkHandler.writeMappingResult(spark, mappingJobExecution, mappingTaskName, dataset, resourceWriter)
      } catch {
        case e: Throwable =>
          // Pass the throwable, NOT e.getMessage: a String selects scala-logging's
          // `error(String, Any*)` overload, whose args fill `{}` placeholders — and this message is
          // fully interpolated, so the argument was silently discarded along with the stack trace.
          // This is the deliberate swallow path (the stream survives bad chunks), so this line is the
          // only record a failed micro-batch ever leaves.
          logger.error(
            s"Streaming chunk resulted in error for project: ${mappingJobExecution.projectId}, job: ${mappingJobExecution.jobId}, execution: ${mappingJobExecution.id}, mappingTask: $mappingTaskName",
            e
          )
      }

    df.writeStream
      // Explicit per-job, per-mapping-task checkpoint dir so distinct streams don't mix up offsets.
      .option("checkpointLocation", mappingJobExecution.getCheckpointDirectory(mappingTaskName))
      .foreachBatch(datasetWrite)
      .start()
  }
}
