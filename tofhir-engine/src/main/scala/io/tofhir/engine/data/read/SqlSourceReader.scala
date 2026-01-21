package io.tofhir.engine.data.read

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.model.exception.FhirMappingException
import io.tofhir.engine.model.{SqlSource, SqlSourceSettings}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

/**
 *
 * @param spark
 */
class SqlSourceReader(spark: SparkSession) extends BaseDataSourceReader[SqlSource, SqlSourceSettings] {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Read the source data from relational database systems through SQL
   *
   * @param mappingSourceBinding     Configuration information for the mapping source
   * @param mappingJobSourceSettings Common settings for the source system
   * @param schema                   Schema for the source data
   * @param timeRange                Time range for the data to read if given
   * @param jobId                    The identifier of mapping job which executes the mapping
   * @return
   */
  override def read(mappingSourceBinding: SqlSource, mappingJobSourceSettings: SqlSourceSettings, schema: Option[StructType],
                    timeRange: Option[(LocalDateTime, LocalDateTime)] = Option.empty, jobId: Option[String]): DataFrame = {

    if (mappingSourceBinding.tableName.isDefined && mappingSourceBinding.query.isDefined) {
      throw FhirMappingException(s"Both table name: ${mappingSourceBinding.tableName.get} and query: ${mappingSourceBinding.query.get} should not be specified at the same time.")
    }
    if (mappingSourceBinding.tableName.isEmpty && mappingSourceBinding.query.isEmpty) {
      throw FhirMappingException(s"Both table name and query cannot be empty at the same time. One of them must be provided.")
    }

    // As in spark jdbc read docs, instead of a full table you could also use a sub-query in parentheses.
    val dbTable: String = mappingSourceBinding.tableName.getOrElse({
      var query: String = mappingSourceBinding.query.get
      if (timeRange.isDefined) {
        val (fromTs, toTs) = timeRange.get
        query = query.replace("$fromTs", "'" + fromTs.toString + "'").replace("$toTs", "'" + toTs.toString + "'")
      }
      s"( $query ) queryGeneratedTable"
    })

  val reader = spark.read
      .format("jdbc")
      .option("url", mappingJobSourceSettings.databaseUrl)
      .option("dbtable", dbTable)
      .option("user", mappingJobSourceSettings.username)
      .option("password", mappingJobSourceSettings.password)
      .options(mappingSourceBinding.options)

    // Apply schema via customSchema if provided
    val readerWithSchema = schema match {
      case Some(s) =>
        reader.option("customSchema", toCustomSchema(s))
      case None =>
        reader
    }
    readerWithSchema.load()
  }

  /**
   * Converts a Spark {@link StructType} schema into a JDBC-compatible
   * {@code customSchema} definition string.
   *
   * <p>
   * Spark's JDBC data source does not have {@code schema(...)}
   * when reading from a database. Instead, it allows enforcing column types
   * at read time via the {@code customSchema} option. This method translates
   * a {@link StructType} into the format expected by that option.
   * </p>
   *
   * <p>
   * Example:
   * <pre>
   * StructType(
   * StructField("id", IntegerType),
   * StructField("value", StringType),
   * StructField("event_time", TimestampType)
   * )
   * </pre>
   *
   * will be converted to:
   *
   * <pre>
   * "id INT, value STRING, event_time TIMESTAMP"
   * </pre>
   * </p>
   *
   * @param schema the Spark {@link StructType} defining the expected column names
   *               and data types
   * @return a string suitable for Spark JDBC reads
   */
  private def toCustomSchema(schema: StructType): String =
    schema.fields
      .map(f => s"${f.name} ${f.dataType.sql}")
      .mkString(", ")

}
