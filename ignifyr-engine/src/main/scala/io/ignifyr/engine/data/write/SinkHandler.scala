package io.ignifyr.engine.data.write

import com.typesafe.scalalogging.Logger
import io.ignifyr.engine.execution.log.ExecutionLogger
import io.ignifyr.engine.execution.processing.ErroneousRecordWriter
import io.ignifyr.engine.model._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator

import java.util

object SinkHandler {
  val logger: Logger = Logger(this.getClass)

  /**
   * Writes the FHIR mapping results to the specified resource writer.
   *
   * @param spark The SparkSession instance.
   * @param mappingJobExecution The execution context of the FHIR mapping job.
   * @param mappingTaskName An optional name for the mapping.
   * @param df The DataFrame containing FHIR mapping results.
   * @param resourceWriter The writer instance to write the FHIR resources.
   */
  def writeMappingResult(
      spark: SparkSession,
      mappingJobExecution: FhirMappingJobExecution,
      mappingTaskName: String,
      df: Dataset[FhirMappingResult],
      resourceWriter: BaseSinkWriter
  ): Unit = {
    // Cache the dataframe
    df.cache()
    // Filter out the errors
    val invalidInputs = df.filter(_.error.map(_.code).contains(FhirMappingErrorCodes.INVALID_INPUT))
    val mappingErrors = df.filter(_.error.exists(_.code != FhirMappingErrorCodes.INVALID_INPUT))
    val mappedResults = df.filter(_.mappedFhirResource.flatMap(_.mappedResource).isDefined)
    // Create an accumulator to accumulate the results that cannot be written
    val accumName = s"${mappingJobExecution.jobId}:$mappingTaskName:fhirWritingProblems"
    val fhirWriteProblemsAccum: CollectionAccumulator[FhirMappingResult] =
      spark.sparkContext.collectionAccumulator[FhirMappingResult](accumName)
    fhirWriteProblemsAccum.reset()
    // Write the FHIR resources
    resourceWriter.write(spark, mappedResults, fhirWriteProblemsAccum)
    logMappingJobResult(
      mappingJobExecution,
      mappingTaskName,
      mappedResults,
      fhirWriteProblemsAccum.value,
      mappingErrors,
      invalidInputs
    )
    ErroneousRecordWriter.saveErroneousRecords(
      spark,
      mappingJobExecution,
      mappingTaskName,
      fhirWriteProblemsAccum.value,
      mappingErrors,
      invalidInputs
    )
    // Unpersist the data frame
    df.unpersist()
  }

  /**
   * Logs mapping job results including the problems regarding to source data, mapping and generated FHIR resources.
   *
   * @param mappingJobExecution The mapping job execution
   * @param mappingTaskName The name of executed mappingTask
   * @param fhirResources written FHIR resources to the configured server
   * @param notWrittenResources The FHIR resource errors
   * @param mappingErrors The mapping errors
   * @param invalidInputs The source data errors
   * */
  private def logMappingJobResult(
      mappingJobExecution: FhirMappingJobExecution,
      mappingTaskName: String,
      fhirResources: Dataset[FhirMappingResult],
      notWrittenResources: util.List[FhirMappingResult],
      mappingErrors: Dataset[FhirMappingResult],
      invalidInputs: Dataset[FhirMappingResult]
  ) = {
    // Get the not written resources
    val numOfInvalids = invalidInputs.count()
    val numOfNotMapped = mappingErrors.count()
    val numOfNotWritten = notWrittenResources.size()
    val numOfFhirResources = fhirResources.count()
    val numOfWritten = numOfFhirResources - numOfNotWritten

    // Log the job result
    if (mappingJobExecution.isStreamingJob) {
      // Log the result for streaming mapping task execution
      ExecutionLogger.logExecutionResultForStreamingMappingTask(
        mappingJobExecution,
        mappingTaskName,
        numOfInvalids,
        numOfNotMapped,
        numOfWritten,
        numOfNotWritten
      )
    } else {
      // Log the result for batch execution
      ExecutionLogger.logExecutionResultForChunk(
        mappingJobExecution,
        mappingTaskName,
        numOfInvalids,
        numOfNotMapped,
        numOfWritten,
        numOfNotWritten
      )
    }

    // Log the mapping and invalid input errors
    if (numOfNotMapped > 0 || numOfInvalids > 0) {
      mappingErrors
        .union(invalidInputs)
        .foreach(r =>
          logger.warn(
            r.copy(executionId = Some(mappingJobExecution.id)).toMapMarker,
            r.copy(executionId = Some(mappingJobExecution.id)).toString
          )
        )
    }
    if (numOfNotWritten > 0)
      notWrittenResources.forEach(r =>
        logger.warn(
          r.copy(executionId = Some(mappingJobExecution.id)).toMapMarker,
          r.copy(executionId = Some(mappingJobExecution.id)).toString
        )
      )
  }
}
