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
}
