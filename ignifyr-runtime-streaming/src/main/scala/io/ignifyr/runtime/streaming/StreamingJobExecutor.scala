package io.ignifyr.runtime.streaming

import com.typesafe.scalalogging.Logger
import io.ignifyr.engine.data.write.SinkWriterFactory
import io.ignifyr.engine.execution.log.ExecutionLogger
import io.ignifyr.engine.model.{
  FhirMappingJobExecution,
  FhirMappingJobResult,
  SinkSettings,
  IdentityServiceSettings,
  MappingJobSourceSettings,
  TerminologyServiceSettings
}
import io.ignifyr.engine.spi.{MappingTaskPipeline, StreamingExecutionProvider}
import org.apache.spark.sql.streaming.StreamingQuery

import scala.concurrent.{ExecutionContext, Future}

/**
 * Streaming execution provider: starts a Spark structured-streaming query per mapping task, reading
 * each task through the engine's [[MappingTaskPipeline]] and writing results with
 * [[StreamingSinkHandler]]. This is the body that previously lived in
 * `FhirMappingJobManager.startMappingJobStream`.
 */
class StreamingJobExecutor extends StreamingExecutionProvider {

  private val logger: Logger = Logger(this.getClass)

  override def startMappingJobStream(
      pipeline: MappingTaskPipeline,
      mappingJobExecution: FhirMappingJobExecution,
      sourceSettings: Map[String, MappingJobSourceSettings],
      sinkSettings: SinkSettings,
      terminologyServiceSettings: Option[TerminologyServiceSettings],
      identityServiceSettings: Option[IdentityServiceSettings]
  )(implicit ec: ExecutionContext): Map[String, Future[StreamingQuery]] = {
    val sinkWriter = SinkWriterFactory.apply(sinkSettings)
    sinkWriter.validate()
    mappingJobExecution.mappingTasks
      .map(t => {
        logger.debug(
          s"Streaming mapping job ${mappingJobExecution.jobId}, mapping name ${t.name} is started and waiting for the data..."
        )
        // log the start of the FHIR mapping task execution
        ExecutionLogger
          .logExecutionStatus(mappingJobExecution, FhirMappingJobResult.STARTED, Some(t.name), isChunkResult = false)
        // Construct a tuple of (mapping name, Future[StreamingQuery])
        t.name ->
          pipeline
            .runMappingTask(
              jobId = mappingJobExecution.jobId,
              task = t,
              sourceSettings = sourceSettings,
              terminologyServiceSettings = terminologyServiceSettings,
              identityServiceSettings = identityServiceSettings,
              executionId = Some(mappingJobExecution.id),
              projectId = Some(mappingJobExecution.projectId)
            )
            .map(ts =>
              StreamingSinkHandler.writeStream(pipeline.sparkSession, mappingJobExecution, ts, sinkWriter, t.name)
            )
            .recover { case e: Throwable =>
              // log the execution status as "FAILURE"
              ExecutionLogger
                .logExecutionStatus(mappingJobExecution, FhirMappingJobResult.FAILURE, Some(t.name), Some(e))
              throw e
            }
      })
      .toMap
  }
}
