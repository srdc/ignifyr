package io.ignifyr.redcap

import com.typesafe.config.ConfigFactory
import io.ignifyr.engine.spi.ExtensionRegistry
import io.ignifyr.server.common.spi.IgnifyrServerExtensions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Verifies the REDCap module is discovered through both extension SPIs when it is on the
 * classpath (no Docker required), and exercises the pure data-dictionary extraction. The
 * endpoint-level import behaviour stays covered by the server's SchemaEndpointTest.
 */
class RedCapExtensionSpec extends AnyFlatSpec with Matchers {

  "The RedCap engine extension" should "register the extract-redcap-schemas CLI command through ServiceLoader" in {
    val provider = ExtensionRegistry.cliCommands.get("extract-redcap-schemas")
    provider shouldBe defined
    provider.get.helpText shouldBe defined
  }

  it should "translate Boot's flag options into the command's positional arguments in order" in {
    val provider = ExtensionRegistry.cliCommands("extract-redcap-schemas")
    provider.argsFromOptions(
      Map("encoding" -> "ISO-8859-9", "data-dictionary" -> "dict.csv", "definition-root-url" -> "http://test")
    ) shouldBe Seq("dict.csv", "http://test", "ISO-8859-9")
    provider.argsFromOptions(Map("data-dictionary" -> "dict.csv")) shouldBe Seq("dict.csv")
  }

  "The RedCap server extension" should "be discovered through ServiceLoader and gate its proxy routes on configuration" in {
    val withConfig =
      IgnifyrServerExtensions.load(
        ConfigFactory.parseString("""ignifyr-redcap { endpoint = "http://localhost:8095/tofhir-redcap" }""")
      )
    val redcap = withConfig.find(_.id == "redcap")
    redcap shouldBe defined
    redcap.get.rootRoutes should have size 1

    val withoutConfig = IgnifyrServerExtensions.load(ConfigFactory.empty())
    withoutConfig.find(_.id == "redcap").get.rootRoutes shouldBe empty
  }

  "RedCapUtil" should "extract a schema per form and inject the record identifier field" in {
    val rows = Seq(
      Map(
        RedCapDataDictionaryColumns.VARIABLE_FIELD_NAME -> "age",
        RedCapDataDictionaryColumns.FORM_NAME -> "test_form",
        RedCapDataDictionaryColumns.FIELD_TYPE -> RedCapDataTypes.TEXT,
        RedCapDataDictionaryColumns.FIELD_LABEL -> "Age",
        RedCapDataDictionaryColumns.REQUIRED_FIELD -> "y",
        RedCapDataDictionaryColumns.TEXT_VALIDATION_TYPE -> RedCapTextValidationTypes.INTEGER
      )
    )
    val definitions = RedCapUtil.extractSchemasAsSchemaDefinitions(rows, "http://test-schema", "record_id")
    definitions should have size 1
    val schema = definitions.head
    schema.id shouldBe "Test_form"
    schema.url shouldBe "http://test-schema/StructureDefinition/Test_form"
    val fields = schema.fieldDefinitions.get
    fields.map(_.id) should contain theSameElementsInOrderAs Seq("record_id", "age")
    fields.foreach(_.isPrimitive shouldBe true)
  }
}
