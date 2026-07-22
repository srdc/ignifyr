package io.ignifyr.engine.spi

import io.ignifyr.engine.model.FhirMappingJobExecution
import io.ignifyr.engine.model.exception.FhirMappingException

/**
 * Lets a connector translate a low-level streaming failure into an actionable message without the
 * engine having to know the connector's client types.
 *
 * The engine tracks streaming queries (plain Spark SQL) in `RunningJobRegistry`, but a failure's
 * root cause is connector-specific (e.g. a Kafka `UnknownTopicOrPartitionException`). Rather than
 * import connector client libraries into the engine, the registry asks each registered descriptor
 * to describe the error; the first that recognizes it supplies a clearer [[FhirMappingException]].
 */
trait StreamingFailureDescriptor {

  /**
   * @return a clearer exception for this failure if this descriptor recognizes it, else None.
   */
  def describe(
      error: Throwable,
      execution: FhirMappingJobExecution,
      mappingTaskName: String
  ): Option[FhirMappingException]
}
