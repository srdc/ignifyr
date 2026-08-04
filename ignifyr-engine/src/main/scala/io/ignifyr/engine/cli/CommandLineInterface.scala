package io.ignifyr.engine.cli

import io.ignifyr.engine.Execution.actorSystem.dispatcher
import io.ignifyr.engine.IgnifyrEngine
import io.ignifyr.engine.cli.command.{CommandExecutionContext, CommandFactory, Load}
import io.ignifyr.engine.execution.{MappingJobLaunch, MappingJobLauncher}
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
   * Run the given mappingJob and exit the process (batch), keep it alive until the streaming
   * queries terminate (streaming), or hand it to the installed scheduler (scheduled).
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
    val mappingJobExecution = FhirMappingJobExecution(job = mappingJob, mappingTasks = mappingJob.mappings)
    new MappingJobLauncher(ignifyrEngine).launch(mappingJob, mappingJobExecution, ignifyrDbFolderPath) match {
      case MappingJobLaunch.Batch(completion) =>
        Await.result(completion, Duration.Inf)
        System.exit(0)
      case MappingJobLaunch.Streaming(queryRegistrations) =>
        // Wait for all Futures (i.e. Streaming Queries) to complete
        Await.result(Future.sequence(queryRegistrations), Duration.Inf)
      case MappingJobLaunch.Scheduled =>
        () // The scheduler's own threads keep the process alive and fire executions per the cron.
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
      case ("--db" | "--db-path") :: value :: tail =>
        nextArg(map ++ Map("db-path" -> value), tail)
      case flag :: value :: tail if flag.startsWith("--") =>
        // Generic `--flag value` pair; command providers translate these into positional args.
        nextArg(map ++ Map(flag.stripPrefix("--") -> value), tail)
      case str :: tail =>
        nextArg(map ++ Map("command" -> str), tail)
    }
  }

}
