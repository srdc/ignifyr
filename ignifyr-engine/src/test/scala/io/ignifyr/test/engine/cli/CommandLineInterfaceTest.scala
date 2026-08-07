package io.ignifyr.test.engine.cli

import io.ignifyr.engine.cli.CommandLineInterface
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Covers `nextArg`, the parser behind every `Boot` invocation: it turns the process arguments into the
 * `command` token plus the `--flag value` pairs that `Boot` and the CLI command providers read.
 */
class CommandLineInterfaceTest extends AnyFlatSpec with Matchers {

  private def parse(args: String*): Map[String, Any] = CommandLineInterface.nextArg(Map(), args.toList)

  "nextArg" should "return an empty map when there are no arguments" in {
    parse() shouldBe empty
  }

  it should "read a bare token as the command" in {
    parse("cli") shouldBe Map("command" -> "cli")
  }

  it should "map a --flag value pair onto the bare flag name" in {
    parse("run", "--job", "jobs/patient.json") shouldBe
      Map("command" -> "run", "job" -> "jobs/patient.json")
  }

  it should "accept both --db and --db-path for the database folder" in {
    parse("run", "--db", "./db") should contain("db-path" -> "./db")
    parse("run", "--db-path", "./db") should contain("db-path" -> "./db")
  }

  it should "collect several flags of an extension-contributed command" in {
    parse(
      "extract-redcap-schemas",
      "--data-dictionary",
      "dictionary.csv",
      "--definition-root-url",
      "http://example.com/fhir",
      "--encoding",
      "utf-8"
    ) shouldBe Map(
      "command" -> "extract-redcap-schemas",
      "data-dictionary" -> "dictionary.csv",
      "definition-root-url" -> "http://example.com/fhir",
      "encoding" -> "utf-8"
    )
  }

  it should "accept flags before the command" in {
    parse("--db-path", "./db", "run") shouldBe Map("db-path" -> "./db", "command" -> "run")
  }

  // Regression: the trailing `--job` used to fall through to the bare-token case and overwrite
  // `command`, so `Boot` reported "unknown command --job" instead of falling back to the configured job.
  it should "keep the command when a trailing flag has no value" in {
    parse("run", "--job") should contain("command" -> "run")
  }

  it should "keep the first bare token as the command" in {
    parse("run", "extra") should contain("command" -> "run")
  }

  it should "let a later flag override an earlier one with the same name" in {
    parse("run", "--job", "first.json", "--job", "second.json") should contain("job" -> "second.json")
  }
}
