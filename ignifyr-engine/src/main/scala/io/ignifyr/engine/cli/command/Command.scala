package io.ignifyr.engine.cli.command

trait Command {
  def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext
}


