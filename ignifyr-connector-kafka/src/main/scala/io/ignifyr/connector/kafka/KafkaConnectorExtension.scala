package io.ignifyr.connector.kafka

import io.ignifyr.engine.data.read.BaseDataSourceReader
import io.ignifyr.engine.model.exception.FhirMappingException
import io.ignifyr.engine.model.{
  FhirMappingJobExecution,
  KafkaSource,
  KafkaSourceSettings,
  MappingJobSourceSettings,
  MappingSourceBinding
}
import io.ignifyr.engine.spi.{IgnifyrExtension, SourceConnector, StreamingFailureDescriptor}
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec

/**
 * Registers the Kafka source connector with the engine via ServiceLoader, plus a
 * [[StreamingFailureDescriptor]] that turns Kafka's `UnknownTopicOrPartitionException` into a
 * message naming the missing topic(s) — logic that previously lived (with a hard Kafka import) in
 * the engine's RunningJobRegistry.
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

  override def streamingFailureDescriptors: Seq[StreamingFailureDescriptor] = Seq(
    new StreamingFailureDescriptor {
      override def describe(
          error: Throwable,
          execution: FhirMappingJobExecution,
          mappingTaskName: String
      ): Option[FhirMappingException] =
        if (hasCause(error, classOf[UnknownTopicOrPartitionException])) {
          val topicNames = execution.mappingTasks
            .find(_.name.contentEquals(mappingTaskName))
            .toSeq
            .flatMap(_.sourceBinding.values)
            .collect { case k: KafkaSource => k.topicName }
            .mkString(", ")
          Some(
            FhirMappingException(
              s"The following Kafka topic(s) specified in the mapping task do not exist: $topicNames"
            )
          )
        } else None
    }
  )

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
