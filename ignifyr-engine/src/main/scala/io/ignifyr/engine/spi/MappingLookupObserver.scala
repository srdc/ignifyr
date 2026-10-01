package io.ignifyr.engine.spi

import io.ignifyr.engine.model.FhirMappingJobExecution
import org.apache.spark.sql.SparkSession

/**
 * Observation hook for the lookups a mapping performs while it runs: terminology-service calls
 * (`trms:translate*`, `trms:lookup*`), concept-map lookups (`mpp:getConcept`) and unit conversions
 * (`mpp:convertAndReturnQuantity`). The engine only reports what it looked up and whether a match was
 * found; what is done with that (aggregation, logging, dashboards) is up to the installed observer.
 * With no observer installed the engine records nothing and pays nothing.
 *
 * Lifecycle, per mapping task execution:
 *  1. [[recorderFor]] is called on the driver when the mapping service of a task (or of a batch chunk,
 *     or of a streaming query) is built. The returned [[LookupRecorder]] is serialized to the Spark
 *     executors together with the mapping service and receives every [[LookupEvent]] there.
 *  1. [[onChunkCompleted]] is called on the driver once the results of a batch chunk / streaming
 *     micro-batch of that task have been computed and written, so the observer can publish what its
 *     recorder collected for that chunk.
 *
 * Only executions with an execution id are observed; previews and test runs are not.
 */
trait MappingLookupObserver {

  /** Driver side: a recorder collecting the lookups of the given task execution. */
  def recorderFor(spark: SparkSession, scope: MappingLookupScope): LookupRecorder

  /**
   * Driver side: a chunk / micro-batch of a task has been computed and written.
   *
   * @param mappingJobExecution The job execution (its id is the [[MappingLookupScope.executionId]])
   * @param mappingTaskName     The name of the mapping task whose chunk completed
   */
  def onChunkCompleted(mappingJobExecution: FhirMappingJobExecution, mappingTaskName: String): Unit
}

/**
 * Receives lookup events on the Spark executors. Implementations must be serializable and thread-safe
 * (e.g. backed by a Spark accumulator).
 */
trait LookupRecorder extends Serializable {
  def record(event: LookupEvent): Unit
}

object LookupRecorder {

  /** Recorder that drops every event; used when no observer is installed or the execution is not observed. */
  object NoOp extends LookupRecorder {
    override def record(event: LookupEvent): Unit = ()
  }

  /** Fans the events out to several recorders (one per installed observer). */
  final case class Composite(recorders: Seq[LookupRecorder]) extends LookupRecorder {
    override def record(event: LookupEvent): Unit = recorders.foreach(_.record(event))
  }
}

/**
 * Identifies the task execution whose lookups are observed.
 *
 * @param jobId           Mapping job identifier
 * @param projectId       Project identifier of the job, if any
 * @param executionId     Identifier of the job execution
 * @param mappingTaskName Name of the mapping task
 */
case class MappingLookupScope(
    jobId: String,
    projectId: Option[String],
    executionId: String,
    mappingTaskName: String
)

/** The kinds of lookup a mapping performs. */
object LookupTypes {

  /** A terminology-service call (`trms:*`). */
  final val TERMINOLOGY = "TERMINOLOGY"

  /** A concept-map context lookup (`mpp:getConcept`). */
  final val CONCEPT = "CONCEPT"

  /** A unit-conversion lookup (`mpp:convertAndReturnQuantity`). */
  final val UNIT = "UNIT"
}

/**
 * A single lookup performed by a mapping.
 *
 * @param lookupType   One of [[LookupTypes]]
 * @param operation    The operation performed, e.g. `translate`, `lookup`, `getConcept`, `convertAndReturnQuantity`
 * @param conceptMap   The concept map used: for concept/unit lookups the file name of the mapping context (e.g.
 *                     `lab-concept-map.csv`, so a context shared by several mappings reports one name whatever alias
 *                     each mapping gives it; the alias only if the context has no file), for terminology translations
 *                     the concept map url (if given)
 * @param sourceSystem Code system of the looked up code (terminology lookups only)
 * @param sourceCode   The looked up code (for unit lookups, the code whose unit is converted)
 * @param targetSystem Code system of the matched code (terminology translations only)
 * @param targetCode   The matched code or value
 * @param conceptColumn The concept map column asked for (concept lookups asking a single column only, e.g.
 *                     `target_code`); absent when the whole concept is asked for. `targetCode` then holds the value
 *                     of this column
 * @param sourceUnit   The unit to be converted (unit lookups only)
 * @param targetUnit   The unit converted to (unit lookups only)
 * @param success      Whether a match was found
 */
case class LookupEvent(
    lookupType: String,
    operation: String,
    conceptMap: Option[String] = None,
    sourceSystem: Option[String] = None,
    sourceCode: Option[String] = None,
    targetSystem: Option[String] = None,
    targetCode: Option[String] = None,
    conceptColumn: Option[String] = None,
    sourceUnit: Option[String] = None,
    targetUnit: Option[String] = None,
    success: Boolean
)
