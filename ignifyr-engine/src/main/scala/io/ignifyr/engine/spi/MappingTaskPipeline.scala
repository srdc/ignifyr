package io.ignifyr.engine.spi

import io.ignifyr.engine.model.{
  FhirMappingResult,
  FhirMappingTask,
  IdentityServiceSettings,
  MappingJobSourceSettings,
  TerminologyServiceSettings
}
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDateTime
import scala.concurrent.Future

/**
 * The engine's mapping-execution surface that runtime capability providers (streaming, scheduling)
 * build on without depending on the concrete job manager. The engine's `FhirMappingJobManager`
 * implements this; a provider reads a task through [[runMappingTask]] and then applies its own
 * sink/lifecycle handling (e.g. a streaming query).
 */
trait MappingTaskPipeline {

  /** The engine's Spark session. */
  def sparkSession: SparkSession

  /**
   * Reads the source(s) for a single mapping task and executes the mapping, yielding the mapping
   * results as a (possibly streaming) dataset.
   */
  def runMappingTask(
      jobId: String,
      task: FhirMappingTask,
      sourceSettings: Map[String, MappingJobSourceSettings],
      terminologyServiceSettings: Option[TerminologyServiceSettings] = None,
      identityServiceSettings: Option[IdentityServiceSettings] = None,
      timeRange: Option[(LocalDateTime, LocalDateTime)] = None,
      executionId: Option[String] = None,
      projectId: Option[String] = None
  ): Future[Dataset[FhirMappingResult]]
}
