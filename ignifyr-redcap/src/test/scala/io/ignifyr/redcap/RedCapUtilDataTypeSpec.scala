package io.ignifyr.redcap

import io.onfhir.api.FHIR_DATA_TYPES
import io.onfhir.definitions.common.model.SimpleStructureDefinition
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import javax.ws.rs.BadRequestException

/**
 * The REDCap field type + text-validation type -> FHIR data type table, exercised through the pure
 * entry point `extractSchemasAsSchemaDefinitions`. This is the widest lookup table in the repo and the
 * only thing standing between a data dictionary and the column types every downstream mapping reads, so
 * a wrong entry produces a schema that parses but types its data incorrectly — never an error.
 */
class RedCapUtilDataTypeSpec extends AnyFlatSpec with Matchers {

  private val definitionRootUrl = "https://ignifyr.io/fhir"

  /** One data-dictionary row, with only the columns the extractor actually reads. */
  private def row(
      fieldType: String,
      textValidationType: Option[String] = None,
      variableName: String = "field1",
      formName: String = "instrument",
      required: String = "",
      fieldLabel: String = "A label",
      fieldNotes: Option[String] = None
  ): Map[String, String] =
    Map(
      RedCapDataDictionaryColumns.FORM_NAME -> formName,
      RedCapDataDictionaryColumns.VARIABLE_FIELD_NAME -> variableName,
      RedCapDataDictionaryColumns.FIELD_TYPE -> fieldType,
      RedCapDataDictionaryColumns.FIELD_LABEL -> fieldLabel,
      RedCapDataDictionaryColumns.REQUIRED_FIELD -> required
    ) ++
      textValidationType.map(RedCapDataDictionaryColumns.TEXT_VALIDATION_TYPE -> _) ++
      fieldNotes.map(RedCapDataDictionaryColumns.FIELD_NOTES -> _)

  /** The extracted field definitions of the single form, excluding the injected record-identifier field. */
  private def fieldsOf(rows: Map[String, String]*): Seq[SimpleStructureDefinition] =
    RedCapUtil
      .extractSchemasAsSchemaDefinitions(rows, definitionRootUrl, recordIdField = "record_id")
      .head
      .fieldDefinitions
      .get
      .filterNot(_.id == "record_id")

  private def fhirTypeOf(fieldType: String, textValidationType: Option[String] = None): String =
    fieldsOf(row(fieldType, textValidationType)).head.dataTypes.get.head.dataType

  "getDataType" should "map every date text-validation type onto a FHIR date" in {
    Seq(
      RedCapTextValidationTypes.DATE_DMY,
      RedCapTextValidationTypes.DATE_MDY,
      RedCapTextValidationTypes.DATE_YMD
    ).foreach(validation => fhirTypeOf(RedCapDataTypes.TEXT, Some(validation)) shouldBe FHIR_DATA_TYPES.DATE)
  }

  it should "map every datetime text-validation type onto a FHIR dateTime" in {
    Seq(
      RedCapTextValidationTypes.DATETIME_DMY,
      RedCapTextValidationTypes.DATETIME_MDY,
      RedCapTextValidationTypes.DATETIME_YMD,
      RedCapTextValidationTypes.DATETIME_SECOND_DMY,
      RedCapTextValidationTypes.DATETIME_SECONDS_MDY,
      RedCapTextValidationTypes.DATETIME_SECONDS_YMD
    ).foreach(validation => fhirTypeOf(RedCapDataTypes.TEXT, Some(validation)) shouldBe FHIR_DATA_TYPES.DATETIME)
  }

  it should "map both time text-validation types onto a FHIR time" in {
    Seq(RedCapTextValidationTypes.TIME, RedCapTextValidationTypes.TIME_MM_SS)
      .foreach(validation => fhirTypeOf(RedCapDataTypes.TEXT, Some(validation)) shouldBe FHIR_DATA_TYPES.TIME)
  }

  it should "map the numeric text-validation types onto FHIR integer and decimal" in {
    fhirTypeOf(RedCapDataTypes.TEXT, Some(RedCapTextValidationTypes.INTEGER)) shouldBe FHIR_DATA_TYPES.INTEGER
    fhirTypeOf(RedCapDataTypes.TEXT, Some(RedCapTextValidationTypes.NUMBER)) shouldBe FHIR_DATA_TYPES.DECIMAL
    fhirTypeOf(RedCapDataTypes.TEXT, Some(RedCapTextValidationTypes.NUMBER_2DP)) shouldBe FHIR_DATA_TYPES.DECIMAL
  }

  it should "map the free-form text validations onto a FHIR string" in {
    Seq(
      RedCapTextValidationTypes.EMAIL,
      RedCapTextValidationTypes.PHONE,
      RedCapTextValidationTypes.ZIP_CODE,
      RedCapTextValidationTypes.POSTAL_CODE_GERMANY
    ).foreach(validation => fhirTypeOf(RedCapDataTypes.TEXT, Some(validation)) shouldBe FHIR_DATA_TYPES.STRING)
  }

  // REDCap leaves the validation column empty for an unvalidated text field, and the column may be absent
  // altogether in an older export. Both mean "plain string".
  it should "map an empty and an absent text validation onto a FHIR string" in {
    fhirTypeOf(RedCapDataTypes.TEXT, Some("")) shouldBe FHIR_DATA_TYPES.STRING
    fhirTypeOf(RedCapDataTypes.TEXT, None) shouldBe FHIR_DATA_TYPES.STRING
  }

  it should "treat a notes field like a text field" in {
    fhirTypeOf(RedCapDataTypes.NOTES, None) shouldBe FHIR_DATA_TYPES.STRING
    fhirTypeOf(RedCapDataTypes.NOTES, Some(RedCapTextValidationTypes.DATE_YMD)) shouldBe FHIR_DATA_TYPES.DATE
  }

  it should "map the multiple-choice field types onto a FHIR code" in {
    Seq(RedCapDataTypes.RADIO, RedCapDataTypes.DROPDOWN, RedCapDataTypes.CHECKBOXES, RedCapDataTypes.SQL)
      .foreach(fieldType => fhirTypeOf(fieldType) shouldBe FHIR_DATA_TYPES.CODE)
  }

  it should "map the boolean field types onto a FHIR boolean" in {
    Seq(RedCapDataTypes.YES_NO, RedCapDataTypes.TRUE_FALSE)
      .foreach(fieldType => fhirTypeOf(fieldType) shouldBe FHIR_DATA_TYPES.BOOLEAN)
  }

  it should "map calc onto a decimal and slider onto an integer" in {
    fhirTypeOf(RedCapDataTypes.CALC) shouldBe FHIR_DATA_TYPES.DECIMAL
    fhirTypeOf(RedCapDataTypes.SLIDER) shouldBe FHIR_DATA_TYPES.INTEGER
  }

  it should "map a file onto base64Binary, or a Signature when it is signed" in {
    fhirTypeOf(RedCapDataTypes.FILE, None) shouldBe FHIR_DATA_TYPES.BASE64BINARY
    fhirTypeOf(RedCapDataTypes.FILE, Some("")) shouldBe FHIR_DATA_TYPES.BASE64BINARY
    fhirTypeOf(RedCapDataTypes.FILE, Some(RedCapTextValidationTypes.SIGNATURE)) shouldBe FHIR_DATA_TYPES.SIGNATURE
  }

  it should "attach the canonical profile url of the mapped type" in {
    fieldsOf(row(RedCapDataTypes.YES_NO)).head.dataTypes.get.head.profiles.get.head shouldBe
      s"http://hl7.org/fhir/StructureDefinition/${FHIR_DATA_TYPES.BOOLEAN}"
  }

  it should "reject an unknown field type" in {
    val thrown = the[IllegalArgumentException] thrownBy fieldsOf(row("hologram"))
    thrown.getMessage should include("Invalid data type: hologram")
  }

  it should "reject an unknown text validation type" in {
    val thrown = the[IllegalArgumentException] thrownBy fieldsOf(row(RedCapDataTypes.TEXT, Some("runes")))
    thrown.getMessage should include("Invalid text validation type for texts: runes")
  }

  it should "reject an unknown text validation type on a file field" in {
    val thrown = the[IllegalArgumentException] thrownBy fieldsOf(row(RedCapDataTypes.FILE, Some("runes")))
    thrown.getMessage should include("Invalid text validation type for files: runes")
  }

  "getCardinality" should "make a checkbox field repeating and everything else single" in {
    val checkbox = fieldsOf(row(RedCapDataTypes.CHECKBOXES)).head
    checkbox.isArray shouldBe true
    checkbox.maxCardinality shouldBe None // "*"

    val radio = fieldsOf(row(RedCapDataTypes.RADIO)).head
    radio.isArray shouldBe false
    radio.maxCardinality shouldBe Some(1)
  }

  it should "require a field only when the dictionary marks it with y" in {
    fieldsOf(row(RedCapDataTypes.TEXT, required = "y")).head.minCardinality shouldBe 1
    fieldsOf(row(RedCapDataTypes.TEXT, required = "")).head.minCardinality shouldBe 0
    fieldsOf(row(RedCapDataTypes.TEXT, required = "n")).head.minCardinality shouldBe 0
  }

  "extractSchemasAsSchemaDefinitions" should "produce one schema per form, named after it" in {
    val schemas = RedCapUtil.extractSchemasAsSchemaDefinitions(
      Seq(
        row(RedCapDataTypes.TEXT, variableName = "a", formName = "demographics"),
        row(RedCapDataTypes.TEXT, variableName = "b", formName = "vitals")
      ),
      definitionRootUrl,
      recordIdField = "record_id"
    )
    schemas.map(_.name) should contain theSameElementsAs Seq("demographics", "vitals")
    schemas.map(_.id) should contain theSameElementsAs Seq("Demographics", "Vitals")
    schemas.map(_.url) should contain(s"$definitionRootUrl/StructureDefinition/Vitals")
  }

  // A descriptive field displays text and returns no data, so REDCap's export omits it and so must the schema.
  it should "omit a descriptive field entirely" in {
    val fields = fieldsOf(
      row(RedCapDataTypes.DESCRIPTIVE, variableName = "banner"),
      row(RedCapDataTypes.TEXT, variableName = "name")
    )
    fields.map(_.id) shouldBe Seq("name")
  }

  it should "inject the record identifier field when the form does not declare it" in {
    val schema = RedCapUtil
      .extractSchemasAsSchemaDefinitions(
        Seq(row(RedCapDataTypes.TEXT, variableName = "name")),
        definitionRootUrl,
        "record_id"
      )
      .head
    val recordId = schema.fieldDefinitions.get.head
    recordId.id shouldBe "record_id"
    recordId.path shouldBe "Instrument.record_id"
    recordId.short shouldBe Some("Record Identifier")
    recordId.dataTypes.get.head.dataType shouldBe FHIR_DATA_TYPES.STRING
    recordId.minCardinality shouldBe 0
    recordId.maxCardinality shouldBe Some(1)
  }

  it should "not inject the record identifier field twice when the form already declares it" in {
    val schema = RedCapUtil
      .extractSchemasAsSchemaDefinitions(
        Seq(row(RedCapDataTypes.TEXT, variableName = "record_id")),
        definitionRootUrl,
        "record_id"
      )
      .head
    schema.fieldDefinitions.get.count(_.id == "record_id") shouldBe 1
  }

  /*
   * `recordIdField` defaults to "" and the CLI (`extract-redcap-schemas`) takes that default — only the
   * server import route passes a real value from a query parameter. The result is an injected field with
   * an empty id and a trailing-dot path. Pinned rather than changed: CLI-extracted schemas already on
   * disk carry exactly this shape, so altering it is a migration decision, not a test fix.
   */
  it should "inject an unnamed record identifier field when none is supplied (the CLI default)" in {
    val schema = RedCapUtil
      .extractSchemasAsSchemaDefinitions(Seq(row(RedCapDataTypes.TEXT, variableName = "name")), definitionRootUrl, "")
      .head
    val recordId = schema.fieldDefinitions.get.head
    recordId.id shouldBe ""
    recordId.path shouldBe "Instrument."
  }

  // REDCap exports the dictionary as UTF-8-with-BOM, so the first column name arrives BOM-prefixed.
  it should "fall back to the BOM-prefixed variable name column" in {
    val bomRow = Map(
      RedCapDataDictionaryColumns.FORM_NAME -> "instrument",
      RedCapDataDictionaryColumns.VARIABLE_FIELD_NAME_WITH_BOM -> "name",
      RedCapDataDictionaryColumns.FIELD_TYPE -> RedCapDataTypes.TEXT,
      RedCapDataDictionaryColumns.FIELD_LABEL -> "Name",
      RedCapDataDictionaryColumns.REQUIRED_FIELD -> ""
    )
    fieldsOf(bomRow).map(_.id) shouldBe Seq("name")
  }

  it should "reject a dictionary with no variable name column at all" in {
    val noNameRow = Map(
      RedCapDataDictionaryColumns.FORM_NAME -> "instrument",
      RedCapDataDictionaryColumns.FIELD_TYPE -> RedCapDataTypes.TEXT,
      RedCapDataDictionaryColumns.FIELD_LABEL -> "Name",
      RedCapDataDictionaryColumns.REQUIRED_FIELD -> ""
    )
    a[BadRequestException] should be thrownBy fieldsOf(noNameRow)
  }

  it should "use the field label as the definition when there are no field notes" in {
    val withNotes = fieldsOf(row(RedCapDataTypes.TEXT, fieldLabel = "Label", fieldNotes = Some("Notes"))).head
    withNotes.short shouldBe Some("Label")
    withNotes.definition shouldBe Some("Notes")

    val withoutNotes = fieldsOf(row(RedCapDataTypes.TEXT, fieldLabel = "Label")).head
    withoutNotes.definition shouldBe Some("Label")
  }
}
