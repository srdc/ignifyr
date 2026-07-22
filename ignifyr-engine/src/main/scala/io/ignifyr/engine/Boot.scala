package io.ignifyr.engine

import com.typesafe.scalalogging.Logger
import io.ignifyr.common.app.AppVersion
import io.ignifyr.engine.cli.CommandLineInterface
import io.ignifyr.engine.cli.command.CommandExecutionContext
import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.spi.{ExtensionHints, ExtensionRegistry}

/**
 * Entrypoint of Ignifyr
 */
object Boot extends App {

  val logger: Logger = Logger(this.getClass)
  logger.info(s"Starting Ignifyr version: ${AppVersion.getVersion}")

  init(args)

  def init(args: Array[String]): Unit = {
    val options = CommandLineInterface.nextArg(Map(), args.toList)
    options.get("command").map(_.asInstanceOf[String]) match {
      // Interactive command line interface
      case None | Some("cli") =>
        val ignifyrEngine = new IgnifyrEngine()
        CommandLineInterface.start(ignifyrEngine, IgnifyrConfig.engineConfig.initialMappingJobFilePath)

      // Run as batch job
      case Some("run") =>
        val ignifyrEngine = new IgnifyrEngine()
        val mappingJobFilePath =
          if (options.contains("job"))
            options.get("job").map(_.asInstanceOf[String])
          else
            IgnifyrConfig.engineConfig.initialMappingJobFilePath

        val ignifyrDbFolderPath =
          if (options.contains("db-path")) options("db-path").asInstanceOf[String]
          else IgnifyrConfig.engineConfig.ignifyrDbFolderPath

        CommandLineInterface.runJob(ignifyrEngine, mappingJobFilePath, ignifyrDbFolderPath)

      // One-shot execution of any registry-contributed command (e.g. the REDCap module's
      // extract-redcap-schemas); the provider translates flags into positional arguments.
      case Some(commandToken) =>
        ExtensionRegistry.cliCommands.get(commandToken) match {
          case Some(provider) =>
            val ignifyrEngine = new IgnifyrEngine()
            val stringOptions = (options - "command").collect { case (key, value: String) => key -> value }
            provider.create().execute(provider.argsFromOptions(stringOptions), CommandExecutionContext(ignifyrEngine))
          case None =>
            println(ExtensionHints.describeCliCommand(commandToken))
            System.exit(1)
        }
    }
  }
}
