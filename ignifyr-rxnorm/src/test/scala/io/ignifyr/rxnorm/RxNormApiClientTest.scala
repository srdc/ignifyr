package io.ignifyr.rxnorm

import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.onfhir.path.FhirPathException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Covers how the client reads RxNorm's responses: which JSON path each call picks its answer out of,
 * and what an absent or malformed answer turns into. The bodies below are trimmed real RxNorm
 * responses served by [[RxNormApiStub]].
 */
class RxNormApiClientTest extends AnyFlatSpec with Matchers with RxNormApiStub {

  override protected val cannedResponses: Map[String, String] = Map(
    "/REST/ndcproperties.json?id=63739054410&ndcstatus=ALL" ->
      """{"ndcPropertyList":{"ndcProperty":[{"ndcItem":"63739-0544-10","rxcui":"313096"}]}}""",
    // RxNorm answers an unknown NDC with an empty object rather than a 404.
    "/REST/ndcproperties.json?id=123&ndcstatus=ALL" -> "{}",
    // A concept whose rxcui entries are placeholders; the client filters "" and "/" out.
    "/REST/ndcproperties.json?id=00000000000&ndcstatus=ALL" ->
      """{"ndcPropertyList":{"ndcProperty":[{"rxcui":""},{"rxcui":"/"},{"rxcui":"111"}]}}""",
    "/REST/rxcui.json?name=aspirin&search=2" -> """{"idGroup":{"name":"aspirin","rxnormId":["1191"]}}""",
    "/REST/rxcui.json?name=nothing&search=2" -> """{"idGroup":{"name":"nothing"}}""",
    "/REST/rxcui/603748/historystatus.json" ->
      """{"rxcuiStatusHistory":{"metaData":{"status":"Active"},"attributes":{"rxcui":"603748"}}}""",
    "/REST/rxcui/999999999/historystatus.json" -> """{"rxcuiStatusHistory":{"metaData":{"status":"UNKNOWN"}}}""",
    "/REST/rxcui/476556/allProperties.json?prop=attributes" ->
      """{"propConceptGroup":{"propConcept":[
        |{"propCategory":"ATTRIBUTES","propName":"Active_ingredient_RxCUI","propValue":"276237"},
        |{"propCategory":"ATTRIBUTES","propName":"Active_ingredient_RxCUI","propValue":"282401"},
        |{"propCategory":"ATTRIBUTES","propName":"Active_ingredient_name","propValue":"emtricitabine"},
        |{"propCategory":"ATTRIBUTES","propName":"Active_ingredient_name","propValue":"tenofovir disoproxil fumarate"},
        |{"propCategory":"ATTRIBUTES","propName":"Numerator_Value","propValue":"200"},
        |{"propCategory":"ATTRIBUTES","propName":"Numerator_Value","propValue":"300"},
        |{"propCategory":"ATTRIBUTES","propName":"Numerator_Units","propValue":"MG"},
        |{"propCategory":"ATTRIBUTES","propName":"Numerator_Units","propValue":"MG"},
        |{"propCategory":"ATTRIBUTES","propName":"Denominator_Value","propValue":"1"},
        |{"propCategory":"ATTRIBUTES","propName":"Denominator_Value","propValue":"1"},
        |{"propCategory":"ATTRIBUTES","propName":"Denominator_Units","propValue":"EA"},
        |{"propCategory":"ATTRIBUTES","propName":"Denominator_Units","propValue":"EA"}
        |]}}""".stripMargin,
    "/REST/rxcui/476/allProperties.json?prop=attributes" -> "{}",
    "/REST/rxcui/276237/property.json?propName=ATC" ->
      """{"propConceptGroup":{"propConcept":[{"propName":"ATC","propValue":"J05AF09"}]}}"""
  )

  private lazy val client = RxNormApiClient(rxNormRootUrl, timeoutInSec = 10)

  "RxNormApiClient" should "get the corresponding RxNorm CUI for a given NDC" in {
    client.findRxConceptIdByNdc("63739054410") shouldBe Seq("313096")
  }

  it should "return nothing for a non-existent NDC" in {
    client.findRxConceptIdByNdc("123") shouldBe Nil
  }

  it should "drop the empty and slash placeholder concept ids" in {
    client.findRxConceptIdByNdc("00000000000") shouldBe Seq("111")
  }

  it should "find a concept id by drug name" in {
    client.findRxConceptIdByName("aspirin") shouldBe Some("1191")
  }

  it should "return nothing when a drug name resolves to no concept" in {
    client.findRxConceptIdByName("nothing") shouldBe None
  }

  it should "get the history status of a known concept" in {
    client.getRxcuiHistoryStatus("603748").isDefined shouldBe true
  }

  // RxNorm answers for an unknown concept too, with status UNKNOWN; the client must not treat that as a hit.
  it should "report no history status for a concept RxNorm does not know" in {
    client.getRxcuiHistoryStatus("999999999") shouldBe None
  }

  it should "get the ingredients of a drug, pairing each property by position" in {
    val result = client.getIngredientProperties("476556")
    result.length shouldBe 2
    (result.head \ "Active_ingredient_RxCUI").extract[String] shouldBe "276237"
    (result.head \ "Active_ingredient_name").extract[String] shouldBe "emtricitabine"
    (result.head \ "Numerator_Value").extract[Int] shouldBe 200
    (result.head \ "Numerator_Units").extract[String] shouldBe "MG"
    (result(1) \ "Active_ingredient_name").extract[String] shouldBe "tenofovir disoproxil fumarate"
  }

  it should "return no ingredients for a concept that has none" in {
    client.getIngredientProperties("476") shouldBe empty
  }

  it should "get the ATC code of a concept" in {
    client.getAtcCode("276237") shouldBe Seq("J05AF09")
  }

  // Any status other than 200 is turned into a FhirPathException naming the root url, because these
  // calls run inside FHIRPath evaluation where that message is all the mapping author will see.
  it should "raise a FhirPathException naming the root url when RxNorm does not answer with 200" in {
    val thrown = the[FhirPathException] thrownBy client.getAtcCode("no-such-concept")
    thrown.getMessage should include(rxNormRootUrl)
    thrown.getMessage should include("404")
  }
}
