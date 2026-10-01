package io.ignifyr.test

import io.onfhir.path.FhirPathEvaluator
import io.ignifyr.IgnifyrTestSpec
import io.ignifyr.engine.mapping.context.MappingContextLoader
import io.ignifyr.engine.mapping.fhirPath.FhirMappingFunctionsFactory
import io.ignifyr.engine.mapping.service.{LocalTerminologyService, ObservingTerminologyService}
import io.ignifyr.engine.model.{
  CodeSystemFile,
  ConceptMapContext,
  ConceptMapFile,
  FhirMapping,
  LocalFhirTerminologyServiceSettings
}
import io.ignifyr.engine.spi.{LookupEvent, LookupRecorder, LookupTypes}
import org.json4s.{JArray, JNull, JObject, JString}
import org.scalatest.flatspec.AsyncFlatSpec

import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

/**
 * Tests that the concept-map, unit-conversion and terminology-service lookups of a mapping are reported to the
 * [[LookupRecorder]] (the coverage-monitoring hook), with the right outcome.
 */
class LookupRecordingTest extends AsyncFlatSpec with IgnifyrTestSpec {

  /** Recorder keeping the events in memory */
  class CapturingRecorder extends LookupRecorder {
    val events: mutable.ArrayBuffer[LookupEvent] = mutable.ArrayBuffer.empty
    override def record(event: LookupEvent): Unit = synchronized(events += event)
  }

  val labResultMapping: FhirMapping =
    mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/lab-results-mapping")
  val mappingContextLoader = new MappingContextLoader

  val terminologyServiceFolderPath: String =
    Paths.get(getClass.getResource("/terminology-service").toURI).normalize().toAbsolutePath.toString
  val localTerminologyService = new LocalTerminologyService(
    LocalFhirTerminologyServiceSettings(
      terminologyServiceFolderPath,
      conceptMapFiles = Seq(
        ConceptMapFile(
          "sample-concept-map.csv",
          "sample-concept-map.csv",
          "http://example.com/fhir/ConceptMap/sample1",
          "http://terminology.hl7.org/ValueSet/v2-0487",
          "http://snomed.info/sct?fhir_vs"
        )
      ),
      codeSystemFiles = Seq(
        CodeSystemFile("sample-concept-map.csv", "sample-code-system.csv", "http://snomed.info/sct")
      )
    )
  )
  val timeout: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)

  "The mpp functions" should "record concept-map lookups with their outcome" in {
    mappingContextLoader.retrieveContext(labResultMapping.context("obsConceptMap")) map { mappingContext =>
      val recorder = new CapturingRecorder
      val evaluator = FhirPathEvaluator()
        .withFunctionLibrary("mpp", new FhirMappingFunctionsFactory(Map("obsConceptMap" -> mappingContext), recorder))

      evaluator.evaluateOptionalString("mpp:getConcept(%obsConceptMap, '1299-7', 'unit')", JNull) shouldBe Some("mL")
      evaluator.evaluateOptionalString("mpp:getConcept(%obsConceptMap, 'UNKNOWN_CODE', 'unit')", JNull) shouldBe None
      evaluator.evaluateOptionalString(
        "mpp:getConcept(%obsConceptMap, '1299-7', 'UNKNOWN_COLUMN')",
        JNull
      ) shouldBe None
      evaluator.evaluateAndReturnJson("mpp:getConcept(%obsConceptMap, '1299-7')", JNull).isDefined shouldBe true
      evaluator.evaluateAndReturnJson("mpp:getConcept(%obsConceptMap, 'UNKNOWN_CODE')", JNull) shouldBe None
      // An empty key is not a lookup
      evaluator.evaluateAndReturnJson("mpp:getConcept(%obsConceptMap, {})", JNull) shouldBe None

      val cm = Some("other-observation-concept-map.csv")
      recorder.events.map(e =>
        (e.lookupType, e.conceptMap, e.sourceCode, e.conceptColumn, e.targetCode, e.success)
      ) shouldBe Seq(
        (LookupTypes.CONCEPT, cm, Some("1299-7"), Some("unit"), Some("mL"), true),
        (LookupTypes.CONCEPT, cm, Some("UNKNOWN_CODE"), Some("unit"), None, false),
        (LookupTypes.CONCEPT, cm, Some("1299-7"), Some("UNKNOWN_COLUMN"), None, false),
        // The whole concept is asked for: no column
        (LookupTypes.CONCEPT, cm, Some("1299-7"), None, None, true),
        (LookupTypes.CONCEPT, cm, Some("UNKNOWN_CODE"), None, None, false)
      )
      recorder.events.forall(e => e.sourceSystem.isEmpty && e.targetSystem.isEmpty) shouldBe true
    }
  }

  it should "record unit conversions with their outcome" in {
    mappingContextLoader.retrieveContext(labResultMapping.context("labResultUnitConversion")) map { mappingContext =>
      val recorder = new CapturingRecorder
      val evaluator = FhirPathEvaluator()
        .withDefaultFunctionLibraries()
        .withFunctionLibrary(
          "mpp",
          new FhirMappingFunctionsFactory(Map("labResultUnitConversion" -> mappingContext), recorder)
        )
      // 1552,g/l,g/dL,"""$this * 0.1"""
      evaluator
        .evaluateAndReturnJson("mpp:convertAndReturnQuantity(%labResultUnitConversion, '1552', 100, 'g/l')", JNull)
        .isDefined shouldBe true
      evaluator.evaluateOptionalString(
        "mpp:convertAndReturnQuantity(%labResultUnitConversion, '1552', 100, 'UNKNOWN_UNIT')",
        JNull
      ) shouldBe None
      // An empty code is not a lookup (and no longer fails)
      evaluator.evaluateOptionalString(
        "mpp:convertAndReturnQuantity(%labResultUnitConversion, {}, 100, 'g/l')",
        JNull
      ) shouldBe None

      recorder.events.map(e =>
        (e.lookupType, e.conceptMap, e.sourceCode, e.sourceUnit, e.targetUnit, e.success)
      ) shouldBe Seq(
        (LookupTypes.UNIT, Some("lab-results-unit-conversion.csv"), Some("1552"), Some("g/l"), Some("g/dL"), true),
        (LookupTypes.UNIT, Some("lab-results-unit-conversion.csv"), Some("1552"), Some("UNKNOWN_UNIT"), None, false)
      )
    }
  }

  it should "identify a context without a file name by its alias" in {
    val recorder = new CapturingRecorder
    val evaluator = FhirPathEvaluator()
      .withFunctionLibrary(
        "mpp",
        new FhirMappingFunctionsFactory(
          Map("inlineMap" -> ConceptMapContext(Map("A" -> Seq(Map("source_code" -> "A", "target_code" -> "B"))))),
          recorder
        )
      )
    evaluator.evaluateOptionalString("mpp:getConcept(%inlineMap, 'A', 'target_code')", JNull) shouldBe Some("B")
    recorder.events.map(_.conceptMap) shouldBe Seq(Some("inlineMap"))
  }

  "An ObservingTerminologyService" should "record translations with the matched target" in {
    val recorder = new CapturingRecorder
    val service = new ObservingTerminologyService(localTerminologyService, recorder)
    val system = "http://terminology.hl7.org/CodeSystem/v2-0487"
    val conceptMapUrl = "http://example.com/fhir/ConceptMap/sample1"

    Await.result(service.translate("ACNE", system, conceptMapUrl), timeout)
    Await.result(service.translate("UNKNOWN", system, conceptMapUrl), timeout)
    Await.result(
      service.translate(JObject("system" -> JString(system), "code" -> JString("ACNE")), conceptMapUrl),
      timeout
    )
    Await.result(
      service.translate(
        JObject("coding" -> JArray(List(JObject("system" -> JString(system), "code" -> JString("UNKNOWN"))))),
        conceptMapUrl
      ),
      timeout
    )

    recorder.events.map(e =>
      (e.lookupType, e.operation, e.conceptMap, e.sourceSystem, e.sourceCode, e.targetSystem, e.targetCode, e.success)
    ) shouldBe Seq(
      (
        LookupTypes.TERMINOLOGY,
        "translate",
        Some(conceptMapUrl),
        Some(system),
        Some("ACNE"),
        Some("http://snomed.info/sct"),
        Some("309068002"),
        true
      ),
      (LookupTypes.TERMINOLOGY, "translate", Some(conceptMapUrl), Some(system), Some("UNKNOWN"), None, None, false),
      (
        LookupTypes.TERMINOLOGY,
        "translate",
        Some(conceptMapUrl),
        Some(system),
        Some("ACNE"),
        Some("http://snomed.info/sct"),
        Some("309068002"),
        true
      ),
      (LookupTypes.TERMINOLOGY, "translate", Some(conceptMapUrl), Some(system), Some("UNKNOWN"), None, None, false)
    )
  }

  it should "record code lookups with their outcome" in {
    val recorder = new CapturingRecorder
    val service = new ObservingTerminologyService(localTerminologyService, recorder)

    Await.result(service.lookup("119323008", "http://snomed.info/sct"), timeout).isDefined shouldBe true
    Await.result(service.lookup("123", "http://snomed.info/sct"), timeout) shouldBe None

    recorder.events.map(e => (e.lookupType, e.operation, e.sourceSystem, e.sourceCode, e.success)) shouldBe Seq(
      (LookupTypes.TERMINOLOGY, "lookup", Some("http://snomed.info/sct"), Some("119323008"), true),
      (LookupTypes.TERMINOLOGY, "lookup", Some("http://snomed.info/sct"), Some("123"), false)
    )
  }

  it should "only accept translation matches with an accepted equivalence" in {
    def translation(equivalence: String): JObject =
      JObject(
        "parameter" -> JArray(
          List(
            JObject("name" -> JString("result"), "valueBoolean" -> org.json4s.JBool(true)),
            JObject(
              "name" -> JString("match"),
              "part" -> JArray(
                List(
                  JObject("name" -> JString("relationship"), "valueCode" -> JString(equivalence)),
                  JObject(
                    "name" -> JString("concept"),
                    "valueCoding" -> JObject("system" -> JString("s"), "code" -> JString("c"))
                  )
                )
              )
            )
          )
        )
      )
    ObservingTerminologyService.acceptedMatch(translation("equivalent")) shouldBe Some(Some("s") -> Some("c"))
    ObservingTerminologyService.acceptedMatch(translation("not-related-to")) shouldBe None
  }
}
