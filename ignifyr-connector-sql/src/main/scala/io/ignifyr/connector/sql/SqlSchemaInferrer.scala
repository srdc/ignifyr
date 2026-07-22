package io.ignifyr.connector.sql

import io.ignifyr.engine.model.{MappingJobSourceSettings, MappingSourceBinding, SqlSource, SqlSourceSettings}
import io.ignifyr.engine.spi.SourceSchemaInferrer
import org.apache.spark.sql.types._

import java.sql.{DriverManager, ResultSetMetaData, SQLException, Types}
import scala.util.Using

/**
 * Infers the schema of a SQL source from JDBC metadata — no data is read. A plain table name goes
 * through `DatabaseMetaData.getColumns`; a query is executed with `LIMIT 0` and its
 * `ResultSetMetaData` is converted. A binding with `preprocessSql` falls back to the engine's
 * generic Spark-read inference (returns None), since only Spark can evaluate the preprocess query.
 */
class SqlSchemaInferrer extends SourceSchemaInferrer {

  override val id: String = "sql"

  override val settingsClass: Class[_ <: MappingJobSourceSettings] = classOf[SqlSourceSettings]

  override def inferSchema(
      sourceBinding: MappingSourceBinding,
      sourceSettings: MappingJobSourceSettings
  ): Option[StructType] = {
    val sqlSourceSettings = sourceSettings.asInstanceOf[SqlSourceSettings]
    sourceBinding match {
      case sqlSource: SqlSource if sqlSource.preprocessSql.isEmpty =>
        Some(
          getTableSchema(
            sqlSourceSettings.databaseUrl,
            sqlSourceSettings.username,
            sqlSourceSettings.password,
            sqlSource.tableName.getOrElse(sqlSource.query.get),
            sqlSource.query.isDefined
          )
        )
      case _: SqlSource =>
        None // A preprocessSql can only be evaluated by Spark; use the generic Spark-read inference.
      case _ =>
        throw new IllegalStateException(
          "Source binding must be SqlSource if the sourceSettings is SqlSourceSettings."
        )
    }
  }

  /**
   * Get the Spark schema from database metadata
   *
   * @param jdbcUrl
   * @param user
   * @param password
   * @param sqlOrTable
   * @param isQuery
   * @return
   */
  private def getTableSchema(
      jdbcUrl: String,
      user: String,
      password: String,
      sqlOrTable: String,
      isQuery: Boolean
  ): StructType = {

    // Helper: splits a fully qualified table name into (schema, table)
    // If there's no dot, returns (null, tableName)
    def splitQualifiedTableName(tableName: String): (String, String) = {
      val parts = tableName.split("\\.", 2)
      if (parts.length == 2) (parts(0), parts(1)) else (null, tableName)
    }

    try {
      Using.Manager { use =>
        val connection =
          try {
            use(DriverManager.getConnection(jdbcUrl, user, password))
          } catch {
            case e: SQLException =>
              throw new RuntimeException(s"Failed to establish JDBC connection: ${e.getMessage}", e)
            case e: Throwable =>
              throw new RuntimeException(s"Unexpected error while establishing JDBC connection: ${e.getMessage}", e)
          }
        if (isQuery) {
          // For SQL queries, execute the query with a limit of zero rows
          val statement = use(connection.createStatement())
          val sqlWithLimit = sqlOrTable.replaceAll("(?i)\\s+LIMIT\\s+\\d+", "") + " LIMIT 0"
          try {
            val rs = use(statement.executeQuery(sqlWithLimit))
            val meta = rs.getMetaData
            val fields = (1 to meta.getColumnCount).map { i =>
              val columnName = meta.getColumnLabel(i)
              val sqlType = meta.getColumnType(i)
              val isNullable = meta.isNullable(i) != ResultSetMetaData.columnNoNulls
              StructField(columnName, mapSqlTypeToSpark(sqlType), nullable = isNullable)
            }
            StructType(fields)
          } catch {
            case e: SQLException =>
              throw new RuntimeException(s"Failed to execute SQL query for schema inference: ${e.getMessage}", e)
            case e: Throwable =>
              throw new RuntimeException(s"Unexpected error during schema inference: ${e.getMessage}", e)
          }
        } else {
          // For a table name, use JDBC metadata. If the table name is qualified,
          // split it into schema (or catalog) and table.
          val (dbSchema, dbTableName) = splitQualifiedTableName(sqlOrTable)
          // For a table name, use JDBC metadata to get the column definitions
          val metaData = connection.getMetaData
          val rs = use(metaData.getColumns(null, dbSchema, dbTableName, null))
          val fields = Iterator
            .continually(rs)
            .takeWhile(_.next())
            .map { rs =>
              val columnName = rs.getString("COLUMN_NAME")
              val sqlType = rs.getInt("DATA_TYPE")
              val isNullable = rs.getString("IS_NULLABLE") == "YES"
              StructField(columnName, mapSqlTypeToSpark(sqlType), nullable = isNullable)
            }
            .toSeq
          StructType(fields)
        }
      }.get
    }
  }

  /**
   * Map SQL data types to Spark data types for schema conversion
   *
   * @param sqlType
   * @return
   */
  private def mapSqlTypeToSpark(sqlType: Int): DataType = sqlType match {
    case Types.VARCHAR | Types.LONGVARCHAR | Types.CHAR | Types.NCHAR | Types.NVARCHAR => StringType
    case Types.INTEGER | Types.TINYINT | Types.SMALLINT => IntegerType
    case Types.BIGINT => LongType
    case Types.FLOAT | Types.REAL => FloatType
    case Types.DOUBLE | Types.NUMERIC | Types.DECIMAL => DoubleType
    case Types.BOOLEAN | Types.BIT => BooleanType
    case Types.DATE => DateType
    case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE => TimestampType
    case Types.TIME | Types.TIME_WITH_TIMEZONE => TimestampType
    case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY => BinaryType
    case Types.ARRAY => ArrayType(StringType) // Default assumption
    case Types.JAVA_OBJECT | Types.STRUCT | Types.OTHER => StringType // Fallback for unknown types
    case _ => StringType // Default for unsupported types
  }
}
