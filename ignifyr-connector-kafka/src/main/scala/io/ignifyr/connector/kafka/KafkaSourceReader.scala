package io.ignifyr.connector.kafka

import com.fasterxml.jackson.core.JsonParseException
import io.onfhir.api.Resource
import io.onfhir.util.JsonFormatter._
import io.ignifyr.engine.data.read.BaseDataSourceReader
import io.ignifyr.engine.model.{KafkaSource, KafkaSourceSettings}
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.{JBool, JDouble, JInt, JLong, JNull}

import java.time.LocalDateTime
import javax.ws.rs.InternalServerErrorException

/**
 *
 * @param spark
 */
class KafkaSourceReader(spark: SparkSession) extends BaseDataSourceReader[KafkaSource, KafkaSourceSettings] {

  /**
   * Read the source data for the given task from Kafka
   *
   * @param mappingSourceBinding     Configuration information for the mapping source
   * @param mappingJobSourceSettings Common settings for the source system
   * @param schema                   Schema for the source data
   * @param timeRange                Time range for the data to read if given
   * @param jobId                    The identifier of mapping job which executes the mapping
   * @return
   */
  override def read(
      mappingSourceBinding: KafkaSource,
      mappingJobSourceSettings: KafkaSourceSettings,
      schema: Option[StructType] = Option.empty,
      timeRange: Option[(LocalDateTime, LocalDateTime)] = Option.empty,
      jobId: Option[String] = Option.empty
  ): DataFrame = {
    import spark.implicits._

    if (schema.isEmpty) {
      throw new IllegalArgumentException("Schema is required for streaming source")
    }

    // Capture the schema itself, not this reader: the UDF is shipped to the executors, and closing over
    // an instance method would drag the SparkSession along with it.
    val sourceSchema = schema.get
    // user-defined function (UDF) to process message (json) coming from the Kafka topic
    val processDataUDF = udf((message: String) => KafkaSourceReader.coerceMessageToSchema(message, sourceSchema))

    // Determine whether to use batch or streaming read mode based on the 'asStream' setting
    if (mappingJobSourceSettings.asStream) {
      spark.readStream // Use streaming mode for continuous ingestion of new messages from Kafka
        .format("kafka")
        .option("kafka.bootstrap.servers", mappingJobSourceSettings.bootstrapServers)
        .option("subscribe", mappingSourceBinding.topicName)
        .option("inferSchema", value = true)
        .options(mappingSourceBinding.options)
        .load()
        .select($"value".cast(StringType)) // change the type of message from binary to string
        .withColumn("value", processDataUDF(col("value"))) // replace 'value' column with the processed data
        .select(from_json($"value", sourceSchema).as("record"))
        .select("record.*")
    } else {
      // Filter out the 'startingOffsets' option as it always supposed to be "earliest" for batch kafka reads
      val filteredOptions = mappingSourceBinding.options.filterNot(o => o._1 == "startingOffsets")
      spark.read // Use read instead of readStream for a batch read
        .format("kafka")
        .option("kafka.bootstrap.servers", mappingJobSourceSettings.bootstrapServers)
        .option("subscribe", mappingSourceBinding.topicName)
        .option("inferSchema", value = true)
        .options(filteredOptions)
        .load()
        .select($"value".cast(StringType)) // change the type of message from binary to string
        .withColumn("value", processDataUDF(col("value"))) // replace 'value' column with the processed data
        .select(from_json($"value", sourceSchema).as("record"))
        .select("record.*")
    }
  }
}

object KafkaSourceReader {

  /**
   * Coerces a Kafka message (JSON) so that its values are acceptable for the schema's data types. REDCap
   * and similar producers send every value as a string, so a field typed `double`/`integer`/`long`/
   * `boolean` in the schema has to be converted before `from_json` would accept it; an empty string means
   * "no value" and becomes null. A value that cannot be converted fails the read rather than reaching the
   * mapping as null.
   *
   * Lives on the companion object so the UDF that calls it carries no reference to the reader instance
   * (and therefore none to the SparkSession) when it is serialized to the executors.
   *
   * @param message the raw Kafka message
   * @param schema  the schema of the source data
   * @return the message with its values coerced to the schema's data types
   */
  private[kafka] def coerceMessageToSchema(message: String, schema: StructType): String = {
    val json: Resource =
      try { // try-catch block needed to handle unparseable json
        message.parseJson
      } catch {
        case e: JsonParseException =>
          throw new InternalServerErrorException("Kafka message is an unparseable JSON", e)
      }

    // process each field so that string values are acceptable (if they are parseable of course) for some data types such as double and integer
    json
      .mapField(field => {
        schema.fields
          .find(p => p.name.contentEquals(field._1))
          .map(fieldType => { // get field type from the schema
            fieldType.dataType match {
              case _: DoubleType =>
                try {
                  field._1 -> {
                    // performs the following conversions:
                    //  JString(v) => JDouble(v)
                    //  JDouble(v) => JDouble(v)
                    //  JString() => JNull
                    field._2.extract[String] match {
                      case str if str.nonEmpty => JDouble(str.toDouble)
                      case _ => JNull
                    }
                  }
                } catch {
                  case e: NumberFormatException =>
                    throw new InternalServerErrorException(s"${field._2} is not a parsable `Double`", e)
                }
              case _: IntegerType =>
                try {
                  field._1 -> {
                    // performs the following conversions:
                    //  JString(v) => JInt(v)
                    //  JInt(v) => JInt(v)
                    //  JString() => JNull
                    field._2.extract[String] match {
                      case str if str.nonEmpty => JInt(str.toInt)
                      case _ => JNull
                    }
                  }
                } catch {
                  case e: NumberFormatException =>
                    throw new InternalServerErrorException(s"${field._2} is not a parsable `Integer`", e)
                }
              case _: LongType =>
                try {
                  field._1 -> {
                    // performs the following conversions:
                    //  JString(v) => JLong(v)
                    //  JLong(v) => JLong(v)
                    //  JString() => JNull
                    field._2.extract[String] match {
                      case str if str.nonEmpty => JLong(str.toLong)
                      case _ => JNull
                    }
                  }
                } catch {
                  case e: NumberFormatException =>
                    throw new InternalServerErrorException(s"${field._2} is not a parsable `Long`", e)
                }
              case _: BooleanType =>
                try {
                  field._1 -> {
                    // performs the following conversions:
                    //  JString(v) => JBool(v)
                    //  JString("0") => JBool(false)
                    //  JString("1") => JBool(true)
                    //  JBool(v) => JBool(v)
                    //  JString() => JNull
                    val bool = field._2.extractOpt[Boolean] // try to extract as boolean
                    bool match {
                      // matches JBool(v)
                      case Some(value) => JBool(value)
                      // try to extract as string
                      case None =>
                        field._2.extractOpt[String] match {
                          // matches JString("0") or matches JString("1")
                          case Some(value) if value.contentEquals("0") || value.contentEquals("1") =>
                            JBool(if (value.contentEquals("0")) false else true)
                          // matches JString(v)
                          case Some(value) if value.nonEmpty => JBool(value.toBoolean)
                          // matches JString()
                          case _ => JNull
                        }
                    }
                  }
                } catch {
                  case e: IllegalArgumentException =>
                    throw new InternalServerErrorException(s"${field._2} is not a parsable `Boolean`", e)
                }
              case _ => field
            }
          })
          .getOrElse(field)
      })
      .toJson
  }
}
