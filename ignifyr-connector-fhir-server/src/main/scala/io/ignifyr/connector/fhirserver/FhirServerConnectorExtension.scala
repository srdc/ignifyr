package io.ignifyr.connector.fhirserver

import io.ignifyr.engine.data.read.BaseDataSourceReader
import io.ignifyr.engine.model.{FhirServerSource, FhirServerSourceSettings, MappingJobSourceSettings, MappingSourceBinding}
import io.ignifyr.engine.spi.{IgnifyrExtension, SourceConnector}
import org.apache.spark.sql.SparkSession

/**
 * Registers the FHIR-server-as-source connector with the engine via ServiceLoader.
 */
class FhirServerConnectorExtension extends IgnifyrExtension {

  override val id: String = "connector-fhir-server"

  override def sourceConnectors: Seq[SourceConnector] = Seq(
    new SourceConnector {
      override val id: String = "fhir-server"
      override val bindingClass: Class[_ <: MappingSourceBinding] = classOf[FhirServerSource]
      override val settingsClass: Class[_ <: MappingJobSourceSettings] = classOf[FhirServerSourceSettings]
      override def createReader(spark: SparkSession): BaseDataSourceReader[_, _] = new FhirServerDataSourceReader(spark)
    }
  )
}
