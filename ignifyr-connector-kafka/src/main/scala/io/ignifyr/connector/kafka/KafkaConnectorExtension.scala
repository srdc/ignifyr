package io.ignifyr.connector.kafka

import io.ignifyr.engine.data.read.BaseDataSourceReader
import io.ignifyr.engine.model.exception.FhirMappingException
import io.ignifyr.engine.model.{
  FhirMappingJobExecution,
  FhirMappingTask,
  KafkaSource,
  KafkaSourceSettings,
  MappingJobSourceSettings,
  MappingSourceBinding
}
import io.ignifyr.engine.spi.{IgnifyrExtension, SourceConnector, SourceFailureDescriptor}
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec

/**
 * Registers the Kafka source connector with the engine via ServiceLoader, plus a
 * [[SourceFailureDescriptor]] that turns Kafka's `UnknownTopicOrPartitionException` into a
 * message naming the missing topic(s) — logic that previously lived (with a hard Kafka import) in
 * the engine's RunningJobRegistry and the server's ExecutionService.
 */
class KafkaConnectorExtension extends IgnifyrExtension {

  override val id: String = "connector-kafka"

  override def sourceConnectors: Seq[SourceConnector] = Seq(
    new SourceConnector {
      override val id: String = "kafka"
      override val bindingClass: Class[_ <: MappingSourceBinding] = classOf[KafkaSource]
      override val settingsClass: Class[_ <: MappingJobSourceSettings] = classOf[KafkaSourceSettings]
      override def createReader(spark: SparkSession): BaseDataSourceReader[_, _] = new KafkaSourceReader(spark)
    }
  )

  override def sourceFailureDescriptors: Seq[SourceFailureDescriptor] = Seq(
    new SourceFailureDescriptor {
      override def describeStreamingFailure(
          error: Throwable,
          execution: FhirMappingJobExecution,
          mappingTaskName: String
      ): Option[FhirMappingException] =
        if (hasCause(error, classOf[UnknownTopicOrPartitionException])) {
          val bindings = execution.mappingTasks
            .find(_.name.contentEquals(mappingTaskName))
            .toSeq
            .flatMap(_.sourceBinding.values)
          Some(missingTopicsException(bindings))
        } else None

      override def describeBatchTaskFailure(
          error: Throwable,
          mappingTask: FhirMappingTask
      ): Option[FhirMappingException] =
        if (hasCause(error, classOf[UnknownTopicOrPartitionException]))
          Some(missingTopicsException(mappingTask.sourceBinding.values.toSeq))
        else None
    }
  )

  /** Builds the "missing topics" message from the Kafka bindings among the given source bindings. */
  private def missingTopicsException(bindings: Seq[MappingSourceBinding]): FhirMappingException = {
    val topicNames = bindings.collect { case k: KafkaSource => k.topicName }.mkString(", ")
    FhirMappingException(
      s"The following Kafka topic(s) specified in the mapping task do not exist: $topicNames"
    )
  }

  /** Walks the cause chain looking for an exception of the given type. */
  @tailrec
  private def hasCause(error: Throwable, causeType: Class[_ <: Throwable]): Boolean =
    error match {
      case null => false
      case e if causeType.isInstance(e) => true
      case e if e.getCause eq e => false
      case e => hasCause(e.getCause, causeType)
    }
}
