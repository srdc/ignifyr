package io.ignifyr.engine.spi

import io.ignifyr.engine.cli.command.Command

/**
 * Contributes an interactive/one-shot CLI command. Registered under [[name]] plus any [[aliases]];
 * unknown command tokens fall back to the engine's `Unknown` command.
 */
trait CliCommandProvider {

  /** Primary command token, e.g. "run". */
  def name: String

  /** Additional tokens that resolve to the same command, e.g. Seq("execute"). */
  def aliases: Seq[String] = Nil

  /** Instantiate the command. */
  def create(): Command

  /**
   * Translate Boot's parsed `--flag value` options into this command's positional arguments, for
   * one-shot (non-interactive) invocations like `<jar> extract-redcap-schemas --data-dictionary …`.
   */
  def argsFromOptions(options: Map[String, String]): Seq[String] = Seq.empty

  /** One help line describing this command, appended to the interactive CLI's `help` output. */
  def helpText: Option[String] = None
}
