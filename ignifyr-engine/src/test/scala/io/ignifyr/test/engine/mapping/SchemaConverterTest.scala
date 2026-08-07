package io.ignifyr.test.engine.mapping

import io.ignifyr.engine.mapping.schema.SchemaConverter
import io.ignifyr.engine.util.MajorFhirVersion
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Pins `fieldsToSchema`, the Spark-column -> FHIR-element table behind schema inference and schema
 * export. It is a plain lookup table with no dependencies, so a silently changed entry produces a
 * wrong element type in an exported StructureDefinition rather than an error.
 *
 * The opposite direction (`convertSchema`) is deliberately not unit-tested here: resolving a
 * StructureDefinition's `differential` needs onFHIR's base FHIR config initialized (the server's
 * `SchemaFolderRepository.initBaseFhirConfig`), so it is covered where that context exists — the
 * server's long-tier `SchemaEndpointTest`. Its half of the mapping is referenced below only as the
 * constant it is, to keep the two directions' known asymmetries visible.
 */
class SchemaConverterTest extends AnyFlatSpec with Matchers {

  private val schemaConverter = new SchemaConverter(MajorFhirVersion.R4)

  private def fhirTypeOf(sparkType: DataType, nullable: Boolean = true): Option[String] =
    schemaConverter
      .fieldsToSchema(StructField("col", sparkType, nullable), "Schema")
      .dataTypes
      .flatMap(_.headOption.map(_.dataType))

  "fieldsToSchema" should "map the Spark integral types onto FHIR integer types" in {
    fhirTypeOf(IntegerType) shouldBe Some("integer")
    fhirTypeOf(ShortType) shouldBe Some("integer")
    fhirTypeOf(ByteType) shouldBe Some("integer")
    fhirTypeOf(LongType) shouldBe Some("integer64")
  }

  it should "map the Spark fractional types onto FHIR decimal" in {
    fhirTypeOf(DoubleType) shouldBe Some("decimal")
    fhirTypeOf(FloatType) shouldBe Some("decimal")
    fhirTypeOf(DataTypes.createDecimalType(10, 2)) shouldBe Some("decimal")
  }

  it should "map the remaining supported Spark types" in {
    fhirTypeOf(StringType) shouldBe Some("string")
    fhirTypeOf(NullType) shouldBe Some("string")
    fhirTypeOf(BooleanType) shouldBe Some("boolean")
    fhirTypeOf(BinaryType) shouldBe Some("base64Binary")
    fhirTypeOf(DateType) shouldBe Some("date")
    fhirTypeOf(TimestampType) shouldBe Some("dateTime")
  }

  it should "attach the canonical profile url of the mapped data type" in {
    schemaConverter
      .fieldsToSchema(StructField("col", BooleanType), "Schema")
      .dataTypes
      .flatMap(_.headOption.flatMap(_.profiles.flatMap(_.headOption))) shouldBe
      Some("http://hl7.org/fhir/StructureDefinition/boolean")
  }

  it should "report no data type for a Spark type it does not know" in {
    fhirTypeOf(StructType(Seq(StructField("nested", StringType)))) shouldBe None
    fhirTypeOf(MapType(StringType, StringType)) shouldBe None
  }

  it should "mark an array element unbounded and a scalar element single" in {
    val array = schemaConverter.fieldsToSchema(StructField("col", ArrayType(StringType)), "Schema")
    array.isArray shouldBe true
    array.maxCardinality shouldBe None // None represents "*"
    array.dataTypes.flatMap(_.headOption.map(_.dataType)) shouldBe Some("string")

    val scalar = schemaConverter.fieldsToSchema(StructField("col", StringType), "Schema")
    scalar.isArray shouldBe false
    scalar.maxCardinality shouldBe Some(1)
  }

  it should "derive the minimum cardinality from nullability" in {
    schemaConverter.fieldsToSchema(StructField("col", StringType, nullable = true), "Schema").minCardinality shouldBe 0
    schemaConverter.fieldsToSchema(StructField("col", StringType, nullable = false), "Schema").minCardinality shouldBe 1
  }

  it should "name the element after the column and prefix its path with the schema" in {
    val element = schemaConverter.fieldsToSchema(StructField("birthDate", DateType), "Ext-patient")
    element.id shouldBe "birthDate"
    element.path shouldBe "Ext-patient.birthDate"
    element.isPrimitive shouldBe true
  }

  /*
   * The read direction (convertSchema) and this write direction are NOT inverses of each other, which is
   * load-bearing knowledge rather than a defect to fix here: changing either table rewrites the
   * StructureDefinitions the server exports for existing projects. The read-direction values quoted below
   * are the ones asserted in SchemaConverter.getSparkType.
   */
  it should "not round-trip the three FHIR types that share a Spark type with another" in {
    fhirTypeOf(TimestampType) shouldBe Some("dateTime") // instant     -> TimestampType -> dateTime
    fhirTypeOf(LongType) shouldBe Some("integer64") // unsignedInt -> LongType      -> integer64
    fhirTypeOf(StringType) shouldBe Some("string") // date/time/code/uri/id -> StringType -> string
  }
}
