package io.ignifyr.test.engine.util

import io.onfhir.client.model.BasicAuthenticationSettings
import io.ignifyr.engine.model._
import io.ignifyr.engine.util.FhirMappingJobFormatter
import org.json4s.MappingException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

/**
 * Covers the mapping-job (de)serializer: the `ShortTypeHints` table that lets one job file carry any
 * source/sink/scheduling type, and the duplicate-mappingTask-name rejection. The server rejects
 * duplicate names on its own create path; this is the file-read path the CLI and the scheduler use.
 */
class FhirMappingJobFormatterTest extends AnyFlatSpec with Matchers {

  private def task(name: String, binding: MappingSourceBinding = FileSystemSource("p.csv", SourceContentTypes.CSV)) =
    FhirMappingTask(name = name, mappingRef = s"http://test/mappings/$name", sourceBinding = Map("source" -> binding))

  /** Round-trips a job through a temp file, the way the CLI reads a job handed to `run --job`. */
  private def roundTrip(job: FhirMappingJob): FhirMappingJob = {
    val file = Files.createTempFile("ignifyr-job", ".json")
    FhirMappingJobFormatter.saveMappingJobToFile(job, file.toString)
    FhirMappingJobFormatter.readMappingJobFromFile(file.toString)
  }

  "findDuplicateMappingTaskNames" should "return nothing when every name is unique" in {
    FhirMappingJobFormatter.findDuplicateMappingTaskNames(Seq(task("a"), task("b"))) shouldBe empty
  }

  it should "return each repeated name once" in {
    val duplicates =
      FhirMappingJobFormatter.findDuplicateMappingTaskNames(Seq(task("a"), task("a"), task("a"), task("b"), task("b")))
    duplicates should contain theSameElementsAs Seq("a", "b")
  }

  "readMappingJobFromFile" should "restore every registered source and sink type" in {
    val job = FhirMappingJob(
      name = Some("multi-source-job"),
      sourceSettings = Map(
        "file" -> FileSystemSourceSettings(name = "f", sourceUri = "urn:f", dataFolderPath = "./data"),
        "sql" -> SqlSourceSettings(
          name = "s",
          sourceUri = "urn:s",
          databaseUrl = "jdbc:h2:mem:t",
          username = "u",
          password = "p"
        ),
        "fhir" -> FhirServerSourceSettings(name = "r", sourceUri = "urn:r", serverUrl = "http://onfhir/fhir")
      ),
      sinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://onfhir/fhir"),
      mappings = Seq(
        task("from-file"),
        task("from-sql", SqlSource(tableName = Some("patients"))),
        task("from-fhir", FhirServerSource(resourceType = "Patient"))
      )
    )

    val restored = roundTrip(job)
    restored.sourceSettings("file") shouldBe a[FileSystemSourceSettings]
    restored.sourceSettings("sql") shouldBe a[SqlSourceSettings]
    restored.sourceSettings("fhir") shouldBe a[FhirServerSourceSettings]
    restored.sinkSettings shouldBe a[FhirRepositorySinkSettings]
    restored.mappings.map(_.sourceBinding("source").getClass) shouldBe
      job.mappings.map(_.sourceBinding("source").getClass)
  }

  it should "restore the sink security settings" in {
    val job = FhirMappingJob(
      sourceSettings = Map("file" -> FileSystemSourceSettings(name = "f", sourceUri = "urn:f", dataFolderPath = "./d")),
      sinkSettings = FhirRepositorySinkSettings(
        fhirRepoUrl = "http://secured/fhir",
        securitySettings = Some(BasicAuthenticationSettings("user", "secret"))
      ),
      mappings = Seq(task("a"))
    )

    val restored = roundTrip(job).sinkSettings.asInstanceOf[FhirRepositorySinkSettings]
    restored.securitySettings shouldBe Some(BasicAuthenticationSettings("user", "secret"))
  }

  it should "restore a file sink with its content type and options" in {
    val sink = FileSystemSinkSettings(
      path = "./out",
      contentType = SinkContentTypes.CSV,
      options = Map("header" -> "true")
    )
    val job = FhirMappingJob(
      sourceSettings = Map("file" -> FileSystemSourceSettings(name = "f", sourceUri = "urn:f", dataFolderPath = "./d")),
      sinkSettings = sink,
      mappings = Seq(task("a"))
    )
    roundTrip(job).sinkSettings shouldBe sink
  }

  it should "reject a job whose mappingTasks share a name" in {
    val job = FhirMappingJob(
      sourceSettings = Map("file" -> FileSystemSourceSettings(name = "f", sourceUri = "urn:f", dataFolderPath = "./d")),
      sinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://onfhir/fhir"),
      mappings = Seq(task("duplicated"), task("duplicated"))
    )
    val file = Files.createTempFile("ignifyr-job", ".json")
    FhirMappingJobFormatter.saveMappingJobToFile(job, file.toString)

    val thrown = the[MappingException] thrownBy FhirMappingJobFormatter.readMappingJobFromFile(file.toString)
    thrown.getMessage should include("duplicated")
    thrown.getMessage should include("unique name")
  }
}
