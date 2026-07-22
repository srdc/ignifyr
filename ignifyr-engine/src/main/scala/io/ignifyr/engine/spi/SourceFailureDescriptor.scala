package io.ignifyr.engine.spi

import io.ignifyr.engine.model.exception.FhirMappingException
import io.ignifyr.engine.model.{FhirMappingJobExecution, FhirMappingTask}

/**
 * Lets a connector translate a low-level source failure into an actionable message without the
 * engine (or the server) having to know the connector's client types.
 *
 * The engine tracks streaming queries (plain Spark SQL) in `RunningJobRegistry`, but a failure's
 * root cause is connector-specific (e.g. a Kafka `UnknownTopicOrPartitionException`). Rather than
 * import connector client libraries into the engine, the registry asks each registered descriptor
 * to describe the error; the first that recognizes it supplies a clearer [[FhirMappingException]].
 * The same applies to ad-hoc batch runs of a single mapping task (e.g. the server's mapping-test
 * endpoint), where the failing read is connector-specific as well.
 */
trait SourceFailureDescriptor {

  /**
   * Describe a streaming-query failure.
   *
   * @return a clearer exception for this failure if this descriptor recognizes it, else None.
   */
  def describeStreamingFailure(
      error: Throwable,
      execution: FhirMappingJobExecution,
      mappingTaskName: String
  ): Option[FhirMappingException]

  /**
   * Describe a failure while reading or executing a single mapping task outside a streaming query
   * (e.g. an ad-hoc mapping-test run forcing a batch read of the source).
   *
   * @return a clearer exception for this failure if this descriptor recognizes it, else None.
   */
  def describeBatchTaskFailure(error: Throwable, mappingTask: FhirMappingTask): Option[FhirMappingException] = None
}
