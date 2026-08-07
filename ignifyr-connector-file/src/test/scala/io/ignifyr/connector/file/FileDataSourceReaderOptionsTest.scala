package io.ignifyr.connector.file

import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.model.{FileSystemSource, FileSystemSourceSettings, SourceContentTypes}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths

/**
 * The two cross-cutting concerns `FileDataSourceReader` applies around the format handler, neither of
 * which any other suite reaches: the `distinct` read option, and how a source path is resolved.
 *
 * Path resolution has a history worth guarding — an earlier `hdfs://` special case further down the
 * write side silently turned parquet output into text — so what is pinned here is the one thing the read
 * side branches on: an `hdfs://` data folder is handed through verbatim, while every other path is
 * resolved against the workspace folder.
 */
class FileDataSourceReaderOptionsTest extends AnyFlatSpec with Matchers {

  private val sparkSession: SparkSession = IgnifyrConfig.sparkSession
  private val reader = new FileDataSourceReader(sparkSession)

  private val testDataFolderPath: String =
    Paths.get(getClass.getResource("/file-data-source-reader-test-data").toURI).toAbsolutePath.toString

  private def readDistinctTest(options: Map[String, String]) =
    reader.read(
      mappingSourceBinding = FileSystemSource(
        path = "patients-with-duplicates.csv",
        contentType = SourceContentTypes.CSV,
        options = options
      ),
      mappingJobSourceSettings = FileSystemSourceSettings(
        name = "test",
        sourceUri = "urn:test",
        dataFolderPath = s"$testDataFolderPath/distinct-test"
      ),
      schema = None
    )

  // The fixture holds 5 rows, of which 2 are exact repeats of earlier ones.
  "the distinct option" should "drop repeated rows when it is set" in {
    readDistinctTest(Map("distinct" -> "true")).count() shouldBe 3
  }

  it should "keep every row when it is absent" in {
    readDistinctTest(Map.empty).count() shouldBe 5
  }

  // Only the exact string "true" enables it; anything else reads the file unchanged.
  it should "keep every row for any value other than true" in {
    readDistinctTest(Map("distinct" -> "false")).count() shouldBe 5
    readDistinctTest(Map("distinct" -> "yes")).count() shouldBe 5
  }

  /*
   * Path resolution is asserted through the streaming directory check, which reports the *resolved*
   * path: it runs straight after resolution and needs no Hadoop filesystem, so the branch can be pinned
   * without an HDFS cluster.
   */
  "path resolution" should "hand an hdfs:// data folder through without prefixing the workspace folder" in {
    val thrown = the[IllegalArgumentException] thrownBy reader.read(
      mappingSourceBinding = FileSystemSource(path = "patients", contentType = SourceContentTypes.CSV),
      mappingJobSourceSettings = FileSystemSourceSettings(
        name = "test",
        sourceUri = "urn:test",
        dataFolderPath = "hdfs://namenode:8020/data",
        asStream = true
      ),
      schema = None
    )
    thrown.getMessage should startWith("hdfs://namenode:8020/data/patients")
  }

  it should "join an hdfs:// folder and path with exactly one separator" in {
    val thrown = the[IllegalArgumentException] thrownBy reader.read(
      mappingSourceBinding = FileSystemSource(path = "/patients", contentType = SourceContentTypes.CSV),
      mappingJobSourceSettings = FileSystemSourceSettings(
        name = "test",
        sourceUri = "urn:test",
        dataFolderPath = "hdfs://namenode:8020/data/",
        asStream = true
      ),
      schema = None
    )
    thrown.getMessage should startWith("hdfs://namenode:8020/data/patients")
  }

  it should "resolve a non-hdfs data folder to an absolute local path" in {
    val thrown = the[IllegalArgumentException] thrownBy reader.read(
      mappingSourceBinding = FileSystemSource(path = "patients.csv", contentType = SourceContentTypes.CSV),
      mappingJobSourceSettings = FileSystemSourceSettings(
        name = "test",
        sourceUri = "urn:test",
        dataFolderPath = s"$testDataFolderPath/single-file-test",
        asStream = true
      ),
      schema = None
    )
    thrown.getMessage should not startWith "hdfs://"
    thrown.getMessage should include("patients.csv")
    thrown.getMessage should include("is not a directory")
  }
}
