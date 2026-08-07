package io.ignifyr.rxnorm

import io.onfhir.path.FhirPathEvaluator
import org.json4s.JsonAST.JNull
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * The `rxn:` FHIRPath functions, resolved against [[RxNormApiStub]] rather than the live RxNorm API.
 * This is the layer a mapping author actually writes against, and it is attached by configuration only
 * (`ignifyr.functionLibraries.rxn.className`), so nothing in Scala would notice if a function stopped
 * resolving.
 */
class RxNormApiFunctionLibraryTest extends AnyFlatSpec with Matchers with RxNormApiStub {

  override protected val cannedResponses: Map[String, String] = Map(
    "/REST/ndcproperties.json?id=63739054410&ndcstatus=ALL" ->
      """{"ndcPropertyList":{"ndcProperty":[{"rxcui":"313096"}]}}""",
    // The library normalises an NDC to 11 digits before calling, so the stub is keyed on the padded form.
    "/REST/ndcproperties.json?id=00000000123&ndcstatus=ALL" -> "{}",
    "/REST/rxcui/276237/property.json?propName=ATC" ->
      """{"propConceptGroup":{"propConcept":[{"propName":"ATC","propValue":"J05AF09"}]}}"""
  )

  private lazy val evaluator =
    FhirPathEvaluator
      .apply()
      .withDefaultFunctionLibraries()
      .withFunctionLibrary("rxn", new RxNormApiFunctionLibraryFactory(rxNormRootUrl, 10))

  "RxNormApiFunctionLibrary" should "resolve findRxConceptIdsByNdc" in {
    evaluator.evaluateOptionalString("rxn:findRxConceptIdsByNdc(63739054410)", JNull) shouldBe Some("313096")
  }

  it should "return nothing for an NDC RxNorm does not know" in {
    evaluator.evaluateOptionalString("rxn:findRxConceptIdsByNdc(123)", JNull) shouldBe None
  }

  // Unlike findRxConceptIdsByNdc, getATC rejects a numeric literal: its rxcui parameter must be a string.
  it should "resolve getATC" in {
    evaluator.evaluateOptionalString("rxn:getATC('276237')", JNull) shouldBe Some("J05AF09")
  }

}
