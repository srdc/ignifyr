package io.ignifyr.engine.cli

import io.ignifyr.engine.Execution.actorSystem.dispatcher
import io.ignifyr.engine.IgnifyrEngine
import io.ignifyr.engine.cli.command.{CommandExecutionContext, CommandFactory, Load}
import io.ignifyr.engine.mapping.job.{FhirMappingJobManager, MappingJobScheduler}
import io.ignifyr.engine.model.FhirMappingJobExecution
import io.ignifyr.engine.util.FhirMappingJobFormatter
import org.json4s.MappingException

import java.io.FileNotFoundException
import java.util.Scanner
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

object CommandLineInterface {

  private var commandExecutionContext: CommandExecutionContext = _

  private def init(ignifyrEngine: IgnifyrEngine, mappingJobFilePath: Option[String]): Unit = {
    this.commandExecutionContext = if (mappingJobFilePath.isDefined) {
      try {
        val mappingJob = FhirMappingJobFormatter.readMappingJobFromFile(mappingJobFilePath.get)
        CommandExecutionContext(
          ignifyrEngine = ignifyrEngine,
          fhirMappingJob = Some(mappingJob),
          mappingNameUrlMap = Load.getTaskNameUrlTuples(mappingJob.mappings, ignifyrEngine.mappingRepo)
        )
      } catch {
        case _: FileNotFoundException =>
          println(s"The file cannot be found at the specified path found in the config:${mappingJobFilePath.get}")
          CommandExecutionContext(ignifyrEngine)
        case _: MappingException =>
          println(s"Invalid MappingJob file at the specified path found in the config:${mappingJobFilePath.get}")
          CommandExecutionContext(ignifyrEngine)
      }
    } else {
      CommandExecutionContext(ignifyrEngine)
    }
  }

  /**
   * Start the interactive CLI so that the user can issue commands through the standard input.
   *
   * @param ignifyrEngine
   * @param mappingJobFilePath
   */
  def start(ignifyrEngine: IgnifyrEngine, mappingJobFilePath: Option[String] = None): Unit = {
    init(ignifyrEngine, mappingJobFilePath)

    print(getWelcomeMessage)
    println()

    val pattern = """[^\s"']+|"([^"]*)"|'([^']*)'""".r // Regex to parse the command and the arguments
    val scanner = new Scanner(System.in)
    print("\n$ ")
    while (scanner.hasNextLine) {
      val userInput = scanner.nextLine()
      val args = pattern
        .findAllMatchIn(userInput)
        .map { m =>
          if (m.group(0).startsWith("\"")) m.group(1) // get rid of the quotes (") at the beginning and the end
          else if (m.group(0).startsWith("\'")) m.group(2) // get rid of the quotes (') at the beginning and the end
          else m.group(0)
        }
        .toSeq
      val commandName = Try(args.head).getOrElse("")
      val commandArgs = Try(args.tail).getOrElse(Seq.empty[String])
      commandExecutionContext = CommandFactory.apply(commandName).execute(commandArgs, commandExecutionContext)
      print("\n$ ")
    }
  }

  private def getWelcomeMessage: String = {
    "Welcome to the CLI of Ignifyr Data Integration Engine\n" +
      "You can use the help command to see available commands and arguments."
  }

  /**
   * Run the given mappingJob as a batch and exit the process.
   *
   * @param ignifyrEngine
   * @param mappingJobFilePath
   */
  def runJob(ignifyrEngine: IgnifyrEngine, mappingJobFilePath: Option[String], ignifyrDbFolderPath: String): Unit = {
    if (mappingJobFilePath.isEmpty) {
      println("There are no jobs to run. Exiting...")
      System.exit(1)
    }
    val mappingJob = FhirMappingJobFormatter.readMappingJobFromFile(mappingJobFilePath.get)
    if (mappingJob.schedulingSettings.isEmpty) {
      val fhirMappingJobManager =
        new FhirMappingJobManager(
          ignifyrEngine.mappingRepo,
          ignifyrEngine.contextLoader,
          ignifyrEngine.schemaLoader,
          ignifyrEngine.functionLibraries,
          ignifyrEngine.sparkSession
        )
      val mappingJobExecution = FhirMappingJobExecution(job = mappingJob, mappingTasks = mappingJob.mappings)
      if (mappingJob.sourceSettings.exists(_._2.asStream)) {
        val streamingQueryInitializationTasks: Seq[Future[Unit]] =
          fhirMappingJobManager
            .startMappingJobStream(
              mappingJobExecution,
              sourceSettings = mappingJob.sourceSettings,
              sinkSettings = mappingJob.sinkSettings,
              terminologyServiceSettings = mappingJob.terminologyServiceSettings,
              identityServiceSettings = mappingJob.getIdentityServiceSettings()
            )
            .map(sq => ignifyrEngine.runningJobRegistry.registerStreamingQuery(mappingJobExecution, sq._1, sq._2))
            .toSeq
        // Wait for all Futures (i.e. Streaming Queries) to complete
        Await.result(Future.sequence(streamingQueryInitializationTasks), Duration.Inf)

      } else {
        val f =
          fhirMappingJobManager
            .executeMappingJob(
              mappingJobExecution,
              sourceSettings = mappingJob.sourceSettings,
              sinkSettings = mappingJob.sinkSettings,
              terminologyServiceSettings = mappingJob.terminologyServiceSettings,
              identityServiceSettings = mappingJob.getIdentityServiceSettings()
            )
        Await.result(f, Duration.Inf)
      }
    } else {
      val mappingJobScheduler: MappingJobScheduler = MappingJobScheduler.instance(ignifyrDbFolderPath)

      val fhirMappingJobManager =
        new FhirMappingJobManager(
          ignifyrEngine.mappingRepo,
          ignifyrEngine.contextLoader,
          ignifyrEngine.schemaLoader,
          ignifyrEngine.functionLibraries,
          ignifyrEngine.sparkSession,
          Some(mappingJobScheduler)
        )
      fhirMappingJobManager
        .scheduleMappingJob(
          mappingJobExecution = FhirMappingJobExecution(job = mappingJob, mappingTasks = mappingJob.mappings),
          sourceSettings = mappingJob.sourceSettings,
          sinkSettings = mappingJob.sinkSettings,
          schedulingSettings = mappingJob.schedulingSettings.get,
          terminologyServiceSettings = mappingJob.terminologyServiceSettings,
          identityServiceSettings = mappingJob.getIdentityServiceSettings()
        )
      mappingJobScheduler.scheduler.start()
    }

  }

  /**
   * Parse the command line arguments.
   *
   * @param map  The map where the argumentName -> value pairs are kept. Start with an empty Map[String, Any].
   * @param list The list of the arguments
   * @return
   */
  @tailrec
  def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
    list match {
      case Nil => map
      case "--job" :: value :: tail =>
        nextArg(map ++ Map("job" -> value), tail)
      case "--mappings" :: value :: tail =>
        nextArg(map ++ Map("mappings" -> value), tail)
      case "--schemas" :: value :: tail =>
        nextArg(map ++ Map("schemas" -> value), tail)
      case ("--db" | "--db-path") :: value :: tail =>
        nextArg(map ++ Map("db-path" -> value), tail)
      case "--data-dictionary" :: value :: tail =>
        nextArg(map ++ Map("data-dictionary" -> value), tail)
      case "--definition-root-url" :: value :: tail =>
        nextArg(map ++ Map("definition-root-url" -> value), tail)
      case "--encoding" :: value :: tail =>
        nextArg(map ++ Map("encoding" -> value), tail)
      case str :: tail =>
        nextArg(map ++ Map("command" -> str), tail)
      case unknown :: _ =>
        println("Unknown argument " + unknown)
        System.exit(1)
        Map.empty
    }
  }

}
