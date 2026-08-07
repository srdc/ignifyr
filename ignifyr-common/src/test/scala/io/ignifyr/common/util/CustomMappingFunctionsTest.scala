package io.ignifyr.common.util

import io.onfhir.path.{FhirPathEvaluator, FhirPathException}
import org.json4s.JsonAST.JNull
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * The `cst:` FHIRPath library. It is wired by class name only
 * (`ignifyr.functionLibraries.cst.className`) and that block is **active** in `ignifyr-server`'s
 * application.conf, so nothing in Scala references it and nothing else would notice a change here.
 */
class CustomMappingFunctionsTest extends AnyFlatSpec with Matchers {

  private val evaluator =
    FhirPathEvaluator
      .apply()
      .withDefaultFunctionLibraries()
      .withFunctionLibrary("cst", new CustomMappingFunctionsFactory())

  /*
   * createTimeSeriesData base64-*encodes* its input and then reads the encoded bytes back pairwise as
   * little-endian shorts. For "AB": bytes [65,66] encode to "QUI=" = [81,85,73,61]; the pairs [81,85]
   * and [73,61] read little-endian are 81 + (85<<8) = 21841 and 73 + (61<<8) = 15689, each widened to
   * Double before being printed.
   */
  "cst:createTimeSeriesData" should "decode the encoded bytes pairwise as little-endian shorts" in {
    evaluator.evaluateOptionalString("cst:createTimeSeriesData('AB')", JNull) shouldBe Some("21841.0 15689.0")
  }

  it should "produce one number per two encoded bytes" in {
    // "ABCDEF" -> 8 base64 characters -> 4 pairs -> 4 numbers.
    evaluator
      .evaluateOptionalString("cst:createTimeSeriesData('ABCDEF')", JNull)
      .map(_.split(" ").length) shouldBe Some(4)
  }

  it should "be deterministic for the same input" in {
    val first = evaluator.evaluateOptionalString("cst:createTimeSeriesData('some payload')", JNull)
    evaluator.evaluateOptionalString("cst:createTimeSeriesData('some payload')", JNull) shouldBe first
  }

  it should "reject an argument expression that does not return a single string" in {
    a[FhirPathException] should be thrownBy
      evaluator.evaluateOptionalString("cst:createTimeSeriesData(42)", JNull)
  }
}
