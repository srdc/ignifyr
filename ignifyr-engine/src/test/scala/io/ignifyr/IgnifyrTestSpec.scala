package io.ignifyr

import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.execution.RunningJobRegistry
import io.ignifyr.engine.mapping._
import io.ignifyr.engine.mapping.context.{IMappingContextLoader, MappingContextLoader}
import io.ignifyr.engine.mapping.schema.SchemaFolderLoader
import io.ignifyr.engine.repository.mapping.{FhirMappingFolderRepository, IFhirMappingRepository}
import io.ignifyr.engine.util.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors, OptionValues}

import java.io.FileWriter
import java.net.URI
import scala.io.Source

trait IgnifyrTestSpec extends Matchers with OptionValues with Inside with Inspectors {

  val repositoryFolderUri: URI = getClass.getResource(IgnifyrConfig.engineConfig.mappingRepositoryFolderPath).toURI
  val mappingRepository: IFhirMappingRepository = new FhirMappingFolderRepository(repositoryFolderUri)

  val contextLoader: IMappingContextLoader = new MappingContextLoader

  val schemaRepositoryURI: URI = getClass.getResource(IgnifyrConfig.engineConfig.schemaRepositoryFolderPath).toURI
  val schemaRepository = new SchemaFolderLoader(schemaRepositoryURI)

  val sparkSession: SparkSession = IgnifyrConfig.sparkSession

  val runningJobRegistry: RunningJobRegistry = new RunningJobRegistry(sparkSession)

  /**
   * Copies the content of a resource file to given location in the context path.
   * @param path The path to the resource file
   * */
  def copyResourceFile(path: String): Unit = {
    // get the content of resource file
    val sourceData = Source.fromResource(path).mkString
    // get the location of resource file according to the context path
    val file = FileUtils.getPath(path).toAbsolutePath.toFile
    // create the parent directories if not exists
    file.getParentFile.mkdirs()
    // create the file
    val fw = new FileWriter(file)
    try fw.write(sourceData)
    finally fw.close()
  }
}
