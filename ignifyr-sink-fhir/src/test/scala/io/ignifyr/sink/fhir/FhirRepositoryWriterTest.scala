package io.ignifyr.sink.fhir

import io.onfhir.api.model.OutcomeIssue
import io.ignifyr.engine.model.FhirRepositorySinkSettings
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Covers the entry-attribution step of the Firely error path. Firely answers a batch in which *any*
 * entry failed with HTTP 400 (onFHIR does not), so the writer has to re-derive which input produced
 * which problem from the `OutcomeIssue` expressions alone. An issue that cannot be attributed is
 * dropped here — that is the behaviour worth pinning, since a wrong index would blame the wrong record.
 *
 * The surrounding write path needs a live FHIR server and is covered by the Docker integration suites
 * in the modules that produce data (`connector-file`, `connector-sql`, `runtime-scheduling`).
 */
class FhirRepositoryWriterTest extends AnyFlatSpec with Matchers {

  private val writer = new FhirRepositoryWriter(FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost/fhir"))

  private def issue(expression: String*): OutcomeIssue =
    OutcomeIssue(severity = "error", code = "invalid", details = None, diagnostics = None, expression = expression)

  "groupOutcomeIssuesByEntryIndex" should "attribute an issue to the bundle entry named in its expression" in {
    val first = issue("Bundle.entry[0].resource.name[0].family")
    val third = issue("Bundle.entry[2].resource.birthDate")
    writer.groupOutcomeIssuesByEntryIndex(Seq(first, third)) shouldBe Map(0 -> Seq(first), 2 -> Seq(third))
  }

  it should "group several issues reported for the same entry" in {
    val one = issue("Bundle.entry[1].resource.gender")
    val two = issue("Bundle.entry[1].resource.birthDate")
    writer.groupOutcomeIssuesByEntryIndex(Seq(one, two)) shouldBe Map(1 -> Seq(one, two))
  }

  it should "read a multi-digit entry index" in {
    writer.groupOutcomeIssuesByEntryIndex(Seq(issue("Bundle.entry[42].resource"))).keySet shouldBe Set(42)
  }

  // Firely does not always return an expression; without one the issue cannot be blamed on any input
  // record, so it is dropped rather than attached to an arbitrary index.
  it should "drop an issue that carries no expression" in {
    writer.groupOutcomeIssuesByEntryIndex(Seq(issue())) shouldBe empty
  }

  it should "drop an issue whose expression does not point at a bundle entry" in {
    writer.groupOutcomeIssuesByEntryIndex(Seq(issue("Patient.name[0].family"))) shouldBe empty
  }

  it should "use the first expression when an issue reports several" in {
    val multi = issue("Bundle.entry[3].resource.gender", "Bundle.entry[7].resource.gender")
    writer.groupOutcomeIssuesByEntryIndex(Seq(multi)) shouldBe Map(3 -> Seq(multi))
  }

  it should "keep the attributable issues and drop only the rest" in {
    val attributable = issue("Bundle.entry[5].resource")
    writer.groupOutcomeIssuesByEntryIndex(Seq(issue(), attributable, issue("Observation.value"))) shouldBe
      Map(5 -> Seq(attributable))
  }

  it should "return an empty map for no issues" in {
    writer.groupOutcomeIssuesByEntryIndex(Seq.empty) shouldBe empty
  }
}
