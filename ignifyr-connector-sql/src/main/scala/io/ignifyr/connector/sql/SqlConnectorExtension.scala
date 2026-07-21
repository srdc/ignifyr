package io.ignifyr.connector.sql

import io.ignifyr.engine.data.read.BaseDataSourceReader
import io.ignifyr.engine.model.{SqlSource, SqlSourceSettings}
import io.ignifyr.engine.spi.{IgnifyrExtension, SourceConnector}
import org.apache.spark.sql.SparkSession

/**
 * Registers the SQL/JDBC source connector with the engine via ServiceLoader.
 */
class SqlConnectorExtension extends IgnifyrExtension {

  override val id: String = "connector-sql"

  override def sourceConnectors: Seq[SourceConnector] = Seq(
    new SourceConnector {
      override val id: String = "sql"
      override val bindingClass: Class[_ <: io.ignifyr.engine.model.MappingSourceBinding] = classOf[SqlSource]
      override val settingsClass: Class[_ <: io.ignifyr.engine.model.MappingJobSourceSettings] =
        classOf[SqlSourceSettings]
      override def createReader(spark: SparkSession): BaseDataSourceReader[_, _] = new SqlSourceReader(spark)
    }
  )
}
