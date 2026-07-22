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
import java.net.{JarURLConnection, URI}
import java.nio.file.{Files, StandardCopyOption}
import scala.io.Source
import scala.jdk.CollectionConverters._

/**
 * Shared test harness: a Spark session, folder-backed mapping/schema repositories, a mapping
 * context loader and a running-job registry, all wired from the classpath test fixtures shipped by
 * ignifyr-testkit (`/test-mappings`, `/test-schemas`). Modules across the reactor mix this in for
 * fixture-driven suites; it lives in this module rather than the engine so the engine's own tests
 * do not create an engine<->testkit reactor cycle.
 */
trait IgnifyrTestSpec extends Matchers with OptionValues with Inside with Inspectors {

  val repositoryFolderUri: URI =
    IgnifyrTestSpec.resolveResourceFolder(IgnifyrConfig.engineConfig.mappingRepositoryFolderPath)
  val mappingRepository: IFhirMappingRepository = new FhirMappingFolderRepository(repositoryFolderUri)

  val contextLoader: IMappingContextLoader = new MappingContextLoader

  val schemaRepositoryURI: URI =
    IgnifyrTestSpec.resolveResourceFolder(IgnifyrConfig.engineConfig.schemaRepositoryFolderPath)
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

object IgnifyrTestSpec {

  /**
   * Resolves a classpath resource folder to a `file:` URI backed by a real on-disk directory, which
   * the folder-based repositories ([[FhirMappingFolderRepository]] / [[SchemaFolderLoader]], both of
   * which call `new File(uri)`) require.
   *
   * When the fixtures live on disk (a `file:` URL, e.g. a module's `target/classes` during a reactor
   * build) the URL is used directly. When they are packaged inside a jar (a `jar:` URL, e.g. a
   * published testkit artifact) the folder is materialized into a temp directory first, since
   * `new File(jar:...!/...)` is not hierarchical and throws. This keeps fixture consumption working
   * both inside the reactor and against a published testkit.
   */
  private[ignifyr] def resolveResourceFolder(resourcePath: String): URI = {
    val url = Option(getClass.getResource(resourcePath))
      .getOrElse(throw new IllegalStateException(s"Test fixture folder not found on the classpath: $resourcePath"))
    url.getProtocol match {
      case "file" => url.toURI
      case "jar" =>
        val connection = url.openConnection().asInstanceOf[JarURLConnection]
        val jarFile = connection.getJarFile
        val entryName = connection.getEntryName.stripSuffix("/") // e.g. "test-mappings"
        val tempRoot = Files.createTempDirectory("ignifyr-testkit-")
        tempRoot.toFile.deleteOnExit()
        jarFile
          .entries()
          .asScala
          .filter(entry => entry.getName == entryName || entry.getName.startsWith(entryName + "/"))
          .foreach { entry =>
            val target = tempRoot.resolve(entry.getName)
            if (entry.isDirectory) Files.createDirectories(target)
            else {
              Files.createDirectories(target.getParent)
              val in = jarFile.getInputStream(entry)
              try Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING)
              finally in.close()
            }
          }
        tempRoot.resolve(entryName).toUri
      case other =>
        throw new IllegalStateException(s"Unsupported resource protocol '$other' for fixture folder $resourcePath")
    }
  }
}
