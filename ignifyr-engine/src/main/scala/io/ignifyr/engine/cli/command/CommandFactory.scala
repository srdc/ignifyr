package io.ignifyr.engine.cli.command

import io.ignifyr.engine.spi.ExtensionRegistry

/**
 * Resolves a CLI [[Command]] by name/alias from the extension registry. Commands are contributed by
 * modules via `CliCommandProvider` (the community engine registers the built-ins; e.g. REDCap's
 * `extract-redcap-schemas` is contributed by its own module). An unrecognised token maps to
 * [[Unknown]], preserving the previous behaviour.
 */
object CommandFactory {

  def apply(command: String): Command =
    ExtensionRegistry.cliCommands.get(command).map(_.create()).getOrElse(new Unknown(command))
}
