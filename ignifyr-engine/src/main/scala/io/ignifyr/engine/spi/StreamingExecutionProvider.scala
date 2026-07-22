package io.ignifyr.engine.spi

import io.ignifyr.engine.model.{
  FhirMappingJobExecution,
  FhirSinkSettings,
  IdentityServiceSettings,
  MappingJobSourceSettings,
  TerminologyServiceSettings
}
import org.apache.spark.sql.streaming.StreamingQuery

import scala.concurrent.{ExecutionContext, Future}

/**
 * Supplies streaming execution as an installable capability. The community engine can build a
 * streaming source dataset, but starting/writing the Spark structured-streaming queries lives in
 * the enterprise `ignifyr-runtime-streaming` module. At most one provider may be installed; a job
 * with `asStream = true` and no provider fails with a `MissingCapabilityException`.
 */
trait StreamingExecutionProvider {

  /**
   * Starts a streaming query per mapping task, reading each through the [[MappingTaskPipeline]] and
   * writing results to the configured sink.
   *
   * @return a map of mapping-task name to the streaming query future, tracked by the engine's
   *         RunningJobRegistry.
   */
  def startMappingJobStream(
      pipeline: MappingTaskPipeline,
      mappingJobExecution: FhirMappingJobExecution,
      sourceSettings: Map[String, MappingJobSourceSettings],
      sinkSettings: FhirSinkSettings,
      terminologyServiceSettings: Option[TerminologyServiceSettings],
      identityServiceSettings: Option[IdentityServiceSettings]
  )(implicit ec: ExecutionContext): Map[String, Future[StreamingQuery]]
}
