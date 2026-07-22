package io.ignifyr.redcap

import io.ignifyr.engine.cli.command.Command
import io.ignifyr.engine.spi.{CliCommandProvider, IgnifyrExtension}

/**
 * Registers the REDCap engine-side contributions with the engine via ServiceLoader: the
 * `extract-redcap-schemas` CLI command (previously registered by the engine's CoreExtension).
 */
class RedCapExtension extends IgnifyrExtension {

  override val id: String = "redcap"

  override def cliCommands: Seq[CliCommandProvider] = Seq(
    new CliCommandProvider {
      override val name: String = "extract-redcap-schemas"

      override def create(): Command = new ExtractRedCapSchemas()

      /** Boot's one-shot mode passes flags; translate them to the command's positional args. */
      override def argsFromOptions(options: Map[String, String]): Seq[String] =
        Seq(options.get("data-dictionary"), options.get("definition-root-url"), options.get("encoding")).flatten

      override def helpText: Option[String] = Some(
        "extract-redcap-schemas <path> <definition-root-url> <encoding> - Extracts schemas from the given REDCap data dictionary file. Schemas will be annotated with the given definition root url. If the encoding of CSV file is different from UTF-8, you should provide it."
      )
    }
  )
}
