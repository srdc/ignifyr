package io.ignifyr.engine.cli.command

class Reload extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    context.ignifyrEngine.mappingRepo.invalidate()
    println("Fhir mapping definitions have been reloaded.")
    context
  }
}
