package io.ignifyr.format.json

import io.ignifyr.connector.file.FileDataSourceReader
import io.ignifyr.connector.file.format.FileFormatRegistry
import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.model.{FileSystemSource, FileSystemSourceSettings, SourceContentTypes}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths

/**
 * Tests the enterprise JSON/NDJSON source format: it is discovered through the file connector's
 * format registry when this module is on the classpath, and the file reader (from connector-file)
 * correctly reads JSON/NDJSON single files, folders, and zip archives through it. These are the
 * JSON/NDJSON cases relocated from the connector's FileDataSourceReaderTest.
 */
class JsonSourceFormatTest extends AnyFlatSpec with Matchers {

  val sparkSession: SparkSession = IgnifyrConfig.sparkSession

  val testDataFolderPath: String =
    Paths.get(getClass.getResource("/file-data-source-reader-test-data").toURI).toAbsolutePath.toString

  val fileDataSourceReader = new FileDataSourceReader(sparkSession)

  "The JSON format" should "be discovered for the json and ndjson content types" in {
    FileFormatRegistry.sourceFormats.keySet should contain allElementsOf
      Seq(SourceContentTypes.JSON, SourceContentTypes.NDJSON)
  }

  it should "correctly read from JSON and NDJSON files" in {
    // Folder including the test files
    val folderPath = "/single-file-test"

    // Define the expected values for validation (Note: Spark reads json columns in alphabetic order)
    val expectedRowNumber = 10
    val expectedColumns = Array("birthDate", "deceasedDateTime", "gender", "homePostalCode", "pid")
    val expectedFirstRow = Row("2000-05-10", null, "male", null, "p1")
    val expectedLastRow = Row("2003-11", null, "female", null, "p10")

    // Define the file names and their corresponding formats to be tested
    val sourceBindingConfigurations = Seq(
      ("patients.json", SourceContentTypes.JSON),
      ("patients-ndjson.txt", SourceContentTypes.NDJSON)
    )
    // Spark options to test if options are working
    val sparkOptions = Map(
      "allowComments" -> "true",
      "distinct" -> "true" // 'distinct' option randomly changes the order of the rows in the result.
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(
      name = s"JsonSourceFormatTest1",
      sourceUri = "test-uri",
      dataFolderPath = testDataFolderPath.concat(folderPath)
    )
    sourceBindingConfigurations.foreach { case (fileName, contentType) =>
      // Define the source binding and settings for reading the file
      val mappingSourceBinding = FileSystemSource(path = fileName, contentType = contentType, options = sparkOptions)
      // Read the data from the specified file
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      // Check that the result contains the first and last row of the source data
      result.collect().contains(expectedFirstRow) shouldBe true
      result.collect().contains(expectedLastRow) shouldBe true
    }
  }

  it should "correctly read multiple files from JSON and NDJSON folders" in {
    // Folder containing the test folders for JSON and NDJSON files
    val folderPath = "/folder-test"

    // Expected values for validation
    val expectedRowNumber = 9
    val expectedColumns = Array("birthDate", "gender", "pid")
    // Expected rows for validation, one row from each file
    val expectedRows = Set(
      Row("2000-05-10", "male", "p1"),
      Row("1999-06-05", "male", "p4"),
      Row("1972-10-25", "female", "p7")
    )

    // A sequence of folder names and content type of the files to be selected
    val sourceBindingConfigurations = Seq(
      ("json", SourceContentTypes.JSON),
      ("txt-ndjson", SourceContentTypes.NDJSON)
    )

    // Loop through each source binding configuration to run the test
    val mappingJobSourceSettings = FileSystemSourceSettings(
      name = "JsonSourceFormatTest2",
      sourceUri = "test-uri",
      dataFolderPath = testDataFolderPath.concat(folderPath)
    )
    sourceBindingConfigurations.foreach { case (folderName, contentType) =>
      // Read the data using the reader and the defined settings
      val mappingSourceBinding = FileSystemSource(path = folderName, contentType = contentType)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.collect().toSet should contain allElementsOf expectedRows
    }
  }

  it should "correctly read from JSON and NDJSON files inside a zip archive" in {

    // Path to the zip file containing the test files
    val folderPath = "/zip-test"

    // Expected values for validation
    val expectedRowNumber = 9
    val expectedColumns = Array("birthDate", "gender", "pid")
    // Expected rows for validation, one row from each file
    val expectedRows = Set(
      Row("2000-05-10", "male", "p1"),
      Row("1999-06-05", "male", "p4"),
      Row("1972-10-25", "female", "p7")
    )

    // Define the zip file names and their corresponding formats to be tested
    val sourceBindingConfigurations = Seq(
      ("json.zip", SourceContentTypes.JSON), // JSON inside zip
      ("txt-ndjson.zip", SourceContentTypes.NDJSON) // Newline-delimited JSON inside zip
    )

    // Spark options for testing (e.g., allowing comments in the files)
    val sparkOptions = Map(
      "allowComments" -> "true"
    )

    // Loop through each zip file and perform the test
    val mappingJobSourceSettings = FileSystemSourceSettings(
      name = "JsonSourceFormatTest3",
      sourceUri = "zip-uri",
      dataFolderPath = testDataFolderPath.concat(folderPath)
    )

    sourceBindingConfigurations.foreach { case (zipFileName, contentType) =>
      // Define the source binding and read the data from the zip file
      val mappingSourceBinding = FileSystemSource(path = zipFileName, contentType = contentType, options = sparkOptions)
      val result: DataFrame = fileDataSourceReader.read(mappingSourceBinding, mappingJobSourceSettings, Option.empty)

      // Validate the result
      result.count() shouldBe expectedRowNumber
      result.columns shouldBe expectedColumns
      result.collect().toSet should contain allElementsOf expectedRows
    }
  }
}
