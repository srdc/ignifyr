package io.ignifyr.engine.cli.command

import io.ignifyr.engine.spi.ExtensionRegistry

class Help extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    // Help lines contributed by installed extension modules (e.g. REDCap's extract-redcap-schemas)
    val extensionCommandHelp = ExtensionRegistry.cliCommands.values.toSeq.distinct
      .flatMap(_.helpText)
      .sorted
      .map(line => s"\t$line\n")
      .mkString
    println(
      "List of available commands:\n" +
        "\tload <path> - Load the Mapping Job definition file from the path.\n" +
        "\treload - Reload the mapping definitions from their source into the mapping repository.\n" +
        "\trun [<url>|<name>] - Run the task(s). Without a parameter, all task of the loaded Mapping Job are run. A specific task can be indicated with its name or URL.\n" +
        "\thelp - See the available commands and their use.\n" +
        "\tlist - Show jobs with at least one running mapping.\n" +
        "\tstop - Stop the execution of the Mapping Job (if any) or a specific Mapping Task associated with a job.\n" +
        extensionCommandHelp +
        "\texit|quit - Exit the program.\n"
    )
    context
  }
}
