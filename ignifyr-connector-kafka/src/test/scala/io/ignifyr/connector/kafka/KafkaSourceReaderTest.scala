package io.ignifyr.connector.kafka

import org.apache.spark.sql.types._
import org.json4s.jackson.JsonMethods
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import javax.ws.rs.InternalServerErrorException

/**
 * Kafka producers (REDCap among them) send every value as a JSON string, so the reader coerces each value
 * to the type its schema declares before `from_json` sees it. An uncoerced value would be dropped as null
 * by `from_json` instead of failing, which is why the conversion rules — and specifically which inputs are
 * an error rather than a null — are worth pinning. The end-to-end read is covered by the long-tier
 * `KafkaStreamingRedcapTest`.
 */
class KafkaSourceReaderTest extends AnyFlatSpec with Matchers {

  private def schemaOf(fieldType: DataType): StructType = StructType(Seq(StructField("field", fieldType)))

  private def coerce(json: String, fieldType: DataType): String =
    KafkaSourceReader.coerceMessageToSchema(json, schemaOf(fieldType))

  /** The coerced value of the single `field`, rendered back as compact JSON. */
  private def valueOf(json: String, fieldType: DataType): String =
    JsonMethods.compact(JsonMethods.render(JsonMethods.parse(coerce(json, fieldType)) \ "field"))

  "coerceMessageToSchema" should "convert a stringified number to a double" in {
    valueOf("""{"field":"1.5"}""", DoubleType) shouldBe "1.5"
  }

  it should "leave an already numeric double alone" in {
    valueOf("""{"field":1.5}""", DoubleType) shouldBe "1.5"
  }

  it should "convert a stringified integer and long" in {
    valueOf("""{"field":"42"}""", IntegerType) shouldBe "42"
    valueOf("""{"field":"9999999999"}""", LongType) shouldBe "9999999999"
  }

  // An empty string is REDCap's "not answered", and it has to become null rather than a parse failure.
  it should "turn an empty string into null for every numeric type" in {
    Seq[DataType](DoubleType, IntegerType, LongType).foreach { fieldType =>
      valueOf("""{"field":""}""", fieldType) shouldBe "null"
    }
  }

  it should "turn an empty string into null for a boolean" in {
    valueOf("""{"field":""}""", BooleanType) shouldBe "null"
  }

  it should "accept a real boolean unchanged" in {
    valueOf("""{"field":true}""", BooleanType) shouldBe "true"
    valueOf("""{"field":false}""", BooleanType) shouldBe "false"
  }

  // REDCap encodes yes/no answers as "1"/"0", which `toBoolean` alone would reject.
  it should "read the 1 and 0 strings as booleans" in {
    valueOf("""{"field":"1"}""", BooleanType) shouldBe "true"
    valueOf("""{"field":"0"}""", BooleanType) shouldBe "false"
  }

  it should "read the true and false strings as booleans" in {
    valueOf("""{"field":"true"}""", BooleanType) shouldBe "true"
    valueOf("""{"field":"false"}""", BooleanType) shouldBe "false"
  }

  it should "leave a field whose schema type needs no coercion untouched" in {
    valueOf("""{"field":"plain text"}""", StringType) shouldBe "\"plain text\""
  }

  it should "leave a field that the schema does not declare untouched" in {
    val coerced = KafkaSourceReader.coerceMessageToSchema("""{"known":"1","unknown":"1"}""", schemaOf(BooleanType))
    coerced should include("\"unknown\":\"1\"")
  }

  it should "fail on a value that cannot be converted to the declared numeric type" in {
    a[InternalServerErrorException] should be thrownBy coerce("""{"field":"not a number"}""", DoubleType)
    a[InternalServerErrorException] should be thrownBy coerce("""{"field":"not a number"}""", IntegerType)
    a[InternalServerErrorException] should be thrownBy coerce("""{"field":"not a number"}""", LongType)
  }

  it should "fail on a value that cannot be converted to a boolean" in {
    a[InternalServerErrorException] should be thrownBy coerce("""{"field":"maybe"}""", BooleanType)
  }

  it should "fail on an unparseable message rather than passing it downstream" in {
    val thrown = the[InternalServerErrorException] thrownBy coerce("{not json", StringType)
    thrown.getMessage should include("unparseable JSON")
  }

  it should "coerce every declared field of a multi-field message" in {
    val schema = StructType(
      Seq(
        StructField("age", IntegerType),
        StructField("weight", DoubleType),
        StructField("consent", BooleanType),
        StructField("name", StringType)
      )
    )
    val coerced =
      JsonMethods.parse(
        KafkaSourceReader.coerceMessageToSchema("""{"age":"7","weight":"12.5","consent":"1","name":"p1"}""", schema)
      )
    (coerced \ "age").values shouldBe 7
    (coerced \ "weight").values shouldBe 12.5
    (coerced \ "consent").values shouldBe true
    (coerced \ "name").values shouldBe "p1"
  }
}
