package io.ignifyr.engine

import com.typesafe.scalalogging.Logger
import io.ignifyr.common.app.AppVersion
import io.ignifyr.engine.cli.command.{CommandExecutionContext, CommandFactory}
import io.ignifyr.engine.cli.CommandLineInterface
import io.ignifyr.engine.config.IgnifyrConfig

/**
 * Entrypoint of Ignifyr
 */
object Boot extends App {

  val logger: Logger = Logger(this.getClass)
  logger.info(s"Starting Ignifyr version: ${AppVersion.getVersion}")

  init(args)

  def init(args: Array[String]): Unit = {
    val options = CommandLineInterface.nextArg(Map(), args.toList)
    // Interactive command line interface
    if (options.isEmpty || !options.contains("command") || options("command").asInstanceOf[String] == "cli") {
      val ignifyrEngine = new IgnifyrEngine()
      CommandLineInterface.start(ignifyrEngine, IgnifyrConfig.engineConfig.initialMappingJobFilePath)
    }
    // Extract schemas from a REDCap data dictionary
    else if (options("command").asInstanceOf[String] == "extract-redcap-schemas") {
      val ignifyrEngine = new IgnifyrEngine()
      // get parameters
      val dataDictionary = options.get("data-dictionary").map(_.asInstanceOf[String])
      val definitionRootUrl = options.get("definition-root-url").map(_.asInstanceOf[String])
      val encoding = options.get("encoding").map(_.asInstanceOf[String])
      val commandArgs: Seq[String] =
        Seq(dataDictionary, definitionRootUrl, encoding).filter(arg => arg.nonEmpty).map(arg => arg.get)
      // run command
      CommandFactory.apply("extract-redcap-schemas").execute(commandArgs, CommandExecutionContext(ignifyrEngine))
    }
    // Run as batch job
    else if (options("command").asInstanceOf[String] == "run") {
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
    }
  }
}
