package io.ignifyr.sink.file

import io.ignifyr.FhirMappingResultFixtures
import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings, SinkContentTypes}
import io.ignifyr.engine.util.FileUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

/**
 * Unit tests for the FileSystemWriter class (community content types: ndjson, parquet, csv). The
 * Delta cases moved to the enterprise ignifyr-format-delta module. The shared test dataset comes from
 * the testkit's FhirMappingResultFixtures.
 */
class FileSystemWriterTest extends AnyFlatSpec with BeforeAndAfterAll {

  /**
   * SparkSession used for the test cases.
   */
  val sparkSession: SparkSession = IgnifyrConfig.sparkSession
  // Standard Spark encoders (e.g. Encoder[Long] for `.as[Long]`); replaces an accidental dependency on
  // a delta-spark encoder, so these community writer tests do not require delta-spark on the classpath.
  import sparkSession.implicits._

  /**
   * A DataFrame containing FHIR mapping results used as test data.
   */
  val df: Dataset[FhirMappingResult] = FhirMappingResultFixtures.sampleFhirMappingResults(sparkSession)

  /**
   * Tests whether FileSystemWriter can write a DataFrame into an NDJSON file.
   *
   * FileSystemSinkSettings used for this test:
   * {
   *  "path": "output-ndjson",
   *  "contentType": "ndjson"
   * }
   *
   * The expected output structure is:
   *
   * > output-ndjson
   *  > part-00000-f746dffd-8346-41d6-9a8b-6c67231ea6bd-c000.txt.crc
   *  > part-00000-f746dffd-8346-41d6-9a8b-6c67231ea6bd-c000.txt
   * */
  it should "write DataFrame into a ndjson file" in {
    // Define the output path for the NDJSON files
    val outputFolderPath = s"${IgnifyrConfig.engineConfig.contextPath}/output-ndjson"
    // Create a FileSystemWriter with NDJSON as the output content type
    val fileSystemWriter = new FileSystemWriter(sinkSettings =
      FileSystemSinkSettings(path = outputFolderPath, contentType = SinkContentTypes.NDJSON)
    )
    // Write the DataFrame to the file system in NDJSON content type
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Read the written NDJSON files back into a DataFrame
    val writtenDf = sparkSession.read
      .json(outputFolderPath)
    // Verify the total record count
    writtenDf.count() shouldBe 15
    // Group by resourceType and count
    val resultDf = writtenDf.groupBy("resourceType").count()
    // Check the count for "Patient" resourceType
    val patientCount = resultDf.filter(col("resourceType") === "Patient").select("count").as[Long].head()
    patientCount shouldBe 10
    // Check the count for "Condition" resourceType
    val conditionCount = resultDf.filter(col("resourceType") === "Condition").select("count").as[Long].head()
    conditionCount shouldBe 5
  }

  /**
   * Tests whether FileSystemWriter can write a DataFrame into an NDJSON file, partitioned by resource type.
   *
   * The test uses the following FileSystemSinkSettings:
   * {
   *  "path": "output-ndjson",
   *  "contentType": "ndjson"
   * }
   *
   * The expected output structure is:
   *
   * > output-ndjson-by-resource
   *  > Condition
   *    > .part-00000-5519e6da-a21c-45e9-b320-d0085e2901b4-c000.txt.crc
   *    > .part-00000-5519e6da-a21c-45e9-b320-d0085e2901b4-c000.txt
   *  > Patient
   *    > .part-00000-ba4e919a-88a0-4158-8d89-58aa45ef149f-c000.txt.crc
   *    > .part-00000-ba4e919a-88a0-4158-8d89-58aa45ef149f-c000.txt
   * */
  it should "write DataFrame as partitioned NDJSON files based on resource type" in {
    // Define the output path for the NDJSON files
    val outputFolderPath = s"${IgnifyrConfig.engineConfig.contextPath}/output-ndjson-by-resource"
    // Instantiate the FileSystemWriter with NDJSON content type and resource type partitioning
    val fileSystemWriter = new FileSystemWriter(sinkSettings =
      FileSystemSinkSettings(
        path = outputFolderPath,
        contentType = SinkContentTypes.NDJSON,
        partitionByResourceType = true
      )
    )
    // Write the DataFrame using the FileSystemWriter
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Verify that the data was correctly written and partitioned under "Condition"
    val conditionDf = sparkSession.read
      .json(s"$outputFolderPath/Condition")
    conditionDf.count() shouldBe 5
    // Verify that the data was correctly written and partitioned under "Patient"
    val patientDf = sparkSession.read
      .json(s"$outputFolderPath/Patient")
    patientDf.count() shouldBe 10
  }

  /**
   * Tests that results whose mapped output carries no `resourceType` discriminator (e.g. flat/tabular
   * mapping outputs) are skipped — not crashed on and not written to a literal "null" directory — when
   * partitioning by resource type.
   */
  it should "skip results without a resourceType discriminator when partitioning by resource type" in {
    val outputFolderPath = s"${IgnifyrConfig.engineConfig.contextPath}/output-ndjson-unrouted"
    // Two results lack the discriminator; the rest of the fixture dataset is routable as usual
    val undiscriminated = df.limit(2).collect().map(_.copy(resourceType = None)).toSeq
    val dfWithUnrouted = df.union(sparkSession.createDataset(undiscriminated))
    val fileSystemWriter = new FileSystemWriter(sinkSettings =
      FileSystemSinkSettings(
        path = outputFolderPath,
        contentType = SinkContentTypes.NDJSON,
        partitionByResourceType = true
      )
    )
    fileSystemWriter.write(
      sparkSession,
      dfWithUnrouted,
      sparkSession.sparkContext.collectionAccumulator[FhirMappingResult]
    )

    // Only the two known resource types are written — no "null" directory for the unrouted results
    val writtenDirs = new java.io.File(outputFolderPath).listFiles().filter(_.isDirectory).map(_.getName).toSet
    writtenDirs shouldBe Set("Patient", "Condition")
    sparkSession.read.json(s"$outputFolderPath/Patient").count() shouldBe 10
    sparkSession.read.json(s"$outputFolderPath/Condition").count() shouldBe 5
  }

  /**
   * Tests whether FileSystemWriter can write a DataFrame into a Parquet file.
   *
   * The test uses the following FileSystemSinkSettings:
   * {
   *  "path": "output-parquet",
   *  "contentType": "parquet"
   * }
   *
   * The expected output structure is:
   *
   * > output-parquet
   *  > part-00000-34382e7e-b916-4495-af23-d5714e921333-c000.snappy.parquet.crc
   *  > part-00000-34382e7e-b916-4495-af23-d5714e921333-c000.snappy.parquet
   * */
  it should "write DataFrame into a parquet file" in {
    // Define the output path for the parquet files
    val outputFolderPath = s"${IgnifyrConfig.engineConfig.contextPath}/output-parquet"
    // Instantiate the FileSystemWriter with Parquet content type
    val fileSystemWriter = new FileSystemWriter(sinkSettings =
      FileSystemSinkSettings(
        path = outputFolderPath,
        contentType = SinkContentTypes.PARQUET
      )
    )
    // Write the DataFrame using the FileSystemWriter
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Read the written Parquet file back into a DataFrame
    val writtenDf = sparkSession.read
      .parquet(outputFolderPath)
    // Verify the total record count
    writtenDf.count() shouldBe 15
    // Group by resourceType and count
    val resultDf = writtenDf.groupBy("resourceType").count()
    // Check the count for "Patient" resourceType
    val patientCount = resultDf.filter(col("resourceType") === "Patient").select("count").as[Long].head()
    patientCount shouldBe 10
    // Check the count for "Condition" resourceType
    val conditionCount = resultDf.filter(col("resourceType") === "Condition").select("count").as[Long].head()
    conditionCount shouldBe 5
  }

  /**
   * Tests whether FileSystemWriter can write a DataFrame into a parquet file, partitioned by resource type.
   *
   * The test uses the following FileSystemSinkSettings:
   * {
   *  "path": "output-parquet-by-resource",
   *  "contentType": "parquet",
   *  "partitionByResourceType": true
   * }
   *
   * The expected output structure is:
   *
   * > output-parquet-by-resource
   *  > Condition
   *    > .part-00000-86f0fcc4-996b-4bb5-bba0-bae44724e988-c000.snappy.parquet.crc
   *    > .part-00000-86f0fcc4-996b-4bb5-bba0-bae44724e988-c000.snappy.parquet
   *  > Patient
   *    > .part-00000-ca276fd5-1c4f-4dba-8610-49e4b652a52d-c000.snappy.parquet.crc
   *    > .part-00000-ca276fd5-1c4f-4dba-8610-49e4b652a52d-c000.snappy.parquet
   * */
  it should "write DataFrame as partitioned parquet files based on resource type" in {
    // Define the output path for the parquet files
    val outputFolderPath = s"${IgnifyrConfig.engineConfig.contextPath}/output-parquet-by-resource"
    // Instantiate the FileSystemWriter with parquet content type and resource type partitioning
    val fileSystemWriter = new FileSystemWriter(sinkSettings =
      FileSystemSinkSettings(
        path = outputFolderPath,
        contentType = SinkContentTypes.PARQUET,
        partitionByResourceType = true
      )
    )
    // Write the DataFrame using the FileSystemWriter
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Verify that the data was correctly written and partitioned under "Condition"
    val conditionDf = sparkSession.read
      .parquet(s"$outputFolderPath/Condition")
    conditionDf.count() shouldBe 5
    // Verify that the data was correctly written and partitioned under "Patient"
    val patientDf = sparkSession.read
      .parquet(s"$outputFolderPath/Patient")
    patientDf.count() shouldBe 10
  }

  /**
   * Tests that a scheme-qualified path is written in the *requested* content type when partitioning by
   * resource type. This is the invariant an earlier `hdfs://` special case broke: it bypassed the format
   * handler and wrote newline-joined raw text, so a parquet (or Delta) sink pointed at an `hdfs://` path
   * silently produced `.txt` files — and it ignored `numOfPartitions`, `options` and `partitioningColumns`
   * for ndjson as well.
   *
   * `file://` stands in for the scheme-handling path here: exercising literal `hdfs://` needs a Hadoop
   * mini-cluster, which this community module deliberately does not depend on. Spark's DataFrameWriter
   * resolves both through the same Hadoop FileSystem abstraction, and there is no longer any per-scheme
   * branch in FileSinkSupport for a scheme to fall into.
   * */
  it should "write partitioned parquet in the requested format for a scheme-qualified URI path" in {
    val outputUri = FileUtils.getPath("output-parquet-file-uri").toAbsolutePath.toUri.toString.stripSuffix("/")
    val fileSystemWriter = new FileSystemWriter(sinkSettings =
      FileSystemSinkSettings(
        path = outputUri,
        contentType = SinkContentTypes.PARQUET,
        partitionByResourceType = true,
        partitioningColumns = Map("Patient" -> List("gender"))
      )
    )
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Reading the output back as parquet must succeed — under the old raw-text path these were .txt files
    sparkSession.read.parquet(s"$outputUri/Condition").count() shouldBe 5
    sparkSession.read.parquet(s"$outputUri/Patient").count() shouldBe 10
    // ...and the configured partitioning columns must be honoured on a scheme-qualified path too
    sparkSession.read.parquet(s"$outputUri/Patient/gender=female").count() shouldBe 5
    sparkSession.read.parquet(s"$outputUri/Patient/gender=male").count() shouldBe 5
  }

  /**
   * Tests whether FileSystemWriter can write a DataFrame into a parquet file, partitioned by different columns.
   *
   * The test uses the following FileSystemSinkSettings:
   * {
   *  "path": "output-parquet-by-partition",
   *  "contentType": "parquet",
   *  "partitionByResourceType": true,
   *  "partitioningColumns": {
   *    "Patient": ["gender"],
   *    "Condition": ["subject.reference"]
   *  }
   * }
   *
   * The expected output structure is:
   *
   * > output-parquet-by-partition
   *  > Condition
   *    > subject.reference=Patient%2F0b3a0b23a0c6e223b941e63787f15a6a
   *      > .part-00000-4a3c7bc0-164c-471c-9ddf-e8117aa445af.c000.snappy.parquet.crc
   *      > part-00000-4a3c7bc0-164c-471c-9ddf-e8117aa445af.c000.snappy.parquet
   *    > subject.reference=Patient%2F0bbad2343eb86d5cdc16a1b292537576
   *      > .part-00000-4a3c7bc0-164c-471c-9ddf-e8117aa445af.c000.snappy.parquet.crc
   *      > part-00000-4a3c7bc0-164c-471c-9ddf-e8117aa445af.c000.snappy.parquet
   *    > subject.reference=Patient%2F7b650be0176d6d29351f84314a5efbe3
   *      > .part-00000-4a3c7bc0-164c-471c-9ddf-e8117aa445af.c000.snappy.parquet.crc
   *      > part-00000-4a3c7bc0-164c-471c-9ddf-e8117aa445af.c000.snappy.parquet
   *    > subject.reference=Patient%2F34dc88d5972fd5472a942fc80f69f35c
   *      > .part-00000-4a3c7bc0-164c-471c-9ddf-e8117aa445af.c000.snappy.parquet.crc
   *      > part-00000-4a3c7bc0-164c-471c-9ddf-e8117aa445af.c000.snappy.parquet
   *    > subject.reference=Patient%2F49d3c335681ab7fb2d4cdf19769655db
   *      > .part-00000-4a3c7bc0-164c-471c-9ddf-e8117aa445af.c000.snappy.parquet.crc
   *      > part-00000-4a3c7bc0-164c-471c-9ddf-e8117aa445af.c000.snappy.parquet
   *  > Patient
   *    > gender=female
   *      > .part-00000-84ddd340-5ee0-41f8-a566-0e480e36870a.c000.snappy.parquet.crc
   *      > .part-00000-84ddd340-5ee0-41f8-a566-0e480e36870a.c000.snappy.parquet
   *    > gender=male
   *      > .part-00000-84ddd340-5ee0-41f8-a566-0e480e36870a.c000.snappy.parquet.crc
   *      > .part-00000-84ddd340-5ee0-41f8-a566-0e480e36870a.c000.snappy.parquet
   * */
  it should "write DataFrame as partitioned parquet files based on Patient's gender and Condition's reference of subject" in {
    // Define the output path for the parquet files
    val outputFolderPath = s"${IgnifyrConfig.engineConfig.contextPath}/output-parquet-by-partition"
    // Instantiate the FileSystemWriter with parquet content type and partitioning
    val fileSystemWriter = new FileSystemWriter(sinkSettings =
      FileSystemSinkSettings(
        path = outputFolderPath,
        contentType = SinkContentTypes.PARQUET,
        partitionByResourceType = true,
        partitioningColumns = Map("Patient" -> List("gender"), "Condition" -> List("subject.reference"))
      )
    )
    // Write the DataFrame using the FileSystemWriter
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Verify that the data was correctly written and partitioned under "Condition"
    val conditionDf = sparkSession.read
      .parquet(s"$outputFolderPath/Condition")
    conditionDf.count() shouldBe 5
    val patientConditionDf = sparkSession.read
      .parquet(s"$outputFolderPath/Condition/subject.reference=Patient%2F49d3c335681ab7fb2d4cdf19769655db")
    patientConditionDf.count() shouldBe 1
    // Verify that the data was correctly written and partitioned under "Patient"
    val femalePatientDf = sparkSession.read
      .parquet(s"$outputFolderPath/Patient/gender=female")
    femalePatientDf.count() shouldBe 5
    val malePatientDf = sparkSession.read
      .parquet(s"$outputFolderPath/Patient/gender=male")
    malePatientDf.count() shouldBe 5
  }

  /**
   * Tests whether FileSystemWriter can write a DataFrame into a csv file.
   *
   * The test uses the following FileSystemSinkSettings:
   * {
   *  "path": "output-csv",
   *  "contentType": "csv",
   *  "options": {
   *    "header": true
   *  }
   * }
   *
   * The expected output structure is:
   *
   * > output-csv
   *  > .part-00000-755e8c9b-ac9b-4348-b81d-8dccfb6aeb56-c000.csv.crc
   *  > .part-00000-755e8c9b-ac9b-4348-b81d-8dccfb6aeb56-c000.csv
   * */
  it should "write DataFrame into a CSV file" in {
    // Define the output path for the csv files
    val outputFolderPath = s"${IgnifyrConfig.engineConfig.contextPath}/output-csv"
    // Instantiate the FileSystemWriter with csv content type
    val fileSystemWriter = new FileSystemWriter(sinkSettings =
      FileSystemSinkSettings(
        path = outputFolderPath,
        contentType = SinkContentTypes.CSV,
        options = Map("header" -> "true")
      )
    )
    // Write the DataFrame using the FileSystemWriter
    fileSystemWriter.write(sparkSession, df, sparkSession.sparkContext.collectionAccumulator[FhirMappingResult])

    // Read the written csv file back into a DataFrame
    val writtenDf = sparkSession.read
      .option("header", value = true)
      .csv(outputFolderPath)
    // Verify the total record count
    writtenDf.count() shouldBe 15
    // Since CSV is a flat content type, the DataFrame should only contain primitive fields, excluding any nested fields.
    writtenDf.columns.length shouldBe 8

    // Group by resourceType and count
    val resultDf = writtenDf.groupBy("resourceType").count()
    // Check the count for "Patient" resourceType
    val patientCount = resultDf.filter(col("resourceType") === "Patient").select("count").as[Long].head()
    patientCount shouldBe 10
    // Check the count for "Condition" resourceType
    val conditionCount = resultDf.filter(col("resourceType") === "Condition").select("count").as[Long].head()
    conditionCount shouldBe 5
  }

  /**
   * After the tests complete, delete the output folders.
   * */
  override protected def afterAll(): Unit = {
    super.afterAll()
    // delete context path
    org.apache.commons.io.FileUtils.deleteDirectory(FileUtils.getPath("").toFile)
  }
}
