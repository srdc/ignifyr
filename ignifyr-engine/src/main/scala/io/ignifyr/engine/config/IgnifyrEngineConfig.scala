package io.ignifyr.engine.config

import scala.util.Try
import com.typesafe.config.Config
import io.ignifyr.engine.util.FileUtils

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.DurationConverters._

class IgnifyrEngineConfig(ignifyrEngineConfig: Config) {

  /** A path to a context file/directory from where any kind of file system reading should start. */
  lazy val contextPath: String = Try(ignifyrEngineConfig.getString("context-path")).getOrElse(".")

  /** Path to the folder where the mappings are kept. */
  lazy val mappingRepositoryFolderPath: String =
    Try(ignifyrEngineConfig.getString("mappings.repository.folder-path")).getOrElse("mappings")

  /** Path to the folder where the mapping context files are kept. */
  lazy val mappingContextRepositoryFolderPath: String =
    Try(ignifyrEngineConfig.getString("mappings.contexts.repository.folder-path")).getOrElse("mapping-contexts")

  /** Path to the folder where the schema definitions are kept. */
  lazy val schemaRepositoryFolderPath: String =
    Try(ignifyrEngineConfig.getString("mappings.schemas.repository.folder-path")).getOrElse("schemas")

  /** Specific FHIR version for schemas in the schema repository. Represents fhirVersion field in the standard StructureDefinition */
  lazy val schemaRepositoryFhirVersion: String =
    Try(ignifyrEngineConfig.getString("mappings.schemas.fhir-version")).getOrElse("4.0.1")

  /** Path to the folder where the job definitions are kept. */
  lazy val jobRepositoryFolderPath: String =
    Try(ignifyrEngineConfig.getString("mapping-jobs.repository.folder-path")).getOrElse("mapping-jobs")

  /** Path to the folder where the terminology system definitions are kept. */
  lazy val terminologySystemFolderPath: String =
    Try(ignifyrEngineConfig.getString("terminology-systems.folder-path")).getOrElse("terminology-systems")

  /** Timeout for a single mapping */
  lazy val mappingTimeout: Duration = Try(ignifyrEngineConfig.getDuration("mappings.timeout").toScala).toOption
    .getOrElse(Duration.apply(5, TimeUnit.SECONDS))

  /** Absolute file path to the MappingJobs file while initiating the Data Integration Suite */
  lazy val initialMappingJobFilePath: Option[String] = Try(
    ignifyrEngineConfig.getString("mapping-jobs.initial-job-file-path")
  ).toOption

  /**
   * Number of partitions for to repartition the source data before executing the mappings for mapping jobs
   */
  lazy val partitionsForMappingJobs: Option[Int] = Try(
    ignifyrEngineConfig.getInt("mapping-jobs.numOfPartitions")
  ).toOption

  /**
   * Max chunk size to execute for batch executions, if number of records exceed this, the source data will be divided into chunks
   */
  lazy val maxChunkSizeForMappingJobs: Option[Long] = Try(
    ignifyrEngineConfig.getLong("mapping-jobs.maxChunkSize")
  ).toOption

  /** The # of FHIR resources in the group while executing (create/update) a FHIR batch operation. */
  lazy val fhirWriterBatchGroupSize: Int =
    Try(ignifyrEngineConfig.getInt("fhir-server-writer.batch-group-size")).getOrElse(10)

  /** Path to the folder which acts as the folder database of Ignifyr*/
  lazy val ignifyrDbFolderPath: String = Try(ignifyrEngineConfig.getString("db-path")).getOrElse("ignifyr-db")

  /** The parent-folder where the data sources of errors received while running mapping are stored. */
  lazy val erroneousRecordsFolder: String = FileUtils
    .getPath(
      Try(ignifyrEngineConfig.getString("archiving.erroneous-records-folder")).getOrElse("erroneous-records-folder")
    )
    .toString

  /** Folder path where the archive of the processed source data is stored. */
  lazy val archiveFolder: String = FileUtils
    .getPath(Try(ignifyrEngineConfig.getString("archiving.archive-folder")).getOrElse("archive-folder"))
    .toString

  /** Period (in milliseconds) to run archiving task for file streaming jobs */
  lazy val streamArchivingFrequency: Int =
    Try(ignifyrEngineConfig.getInt("archiving.stream-archiving-frequency")).toOption.getOrElse(5000)

  /** Configuration of external function libraries */
  lazy val functionLibrariesConfig: Option[FunctionLibrariesConfig] = Try(
    new FunctionLibrariesConfig(ignifyrEngineConfig.getConfig("functionLibraries"))
  ).toOption
}
