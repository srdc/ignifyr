package io.ignifyr.engine.spi.core

import io.onfhir.api.service.{IFhirIdentityService, IFhirTerminologyService}
import io.onfhir.client.{IdentityServiceClient, TerminologyServiceClient}
import io.ignifyr.engine.cli.command.{
  Command,
  Exit,
  ExtractRedCapSchemas,
  Help,
  ListRunningMappings,
  Load,
  Reload,
  Run,
  Stop
}
import io.ignifyr.engine.data.read.{
  BaseDataSourceReader,
  FhirServerDataSourceReader,
  FileDataSourceReader,
  KafkaSourceReader,
  SqlSourceReader
}
import io.ignifyr.engine.data.write.{BaseFhirWriter, FhirRepositoryWriter, FileSystemWriter}
import io.ignifyr.engine.mapping.service.LocalTerminologyService
import io.ignifyr.engine.model._
import io.ignifyr.engine.spi._
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext

/**
 * The engine's own [[IgnifyrExtension]], registered through the same ServiceLoader mechanism as
 * every other module (no special-casing in the registries). It contributes everything that ships
 * in the community engine today; features earmarked for other editions are peeled off into their
 * own modules by later migration phases, each move being a plain relocation of the corresponding
 * provider out of this file.
 */
class CoreExtension extends IgnifyrExtension {

  override val id: String = "core"

  override def sourceConnectors: Seq[SourceConnector] = Seq(
    source("file", classOf[FileSystemSource], classOf[FileSystemSourceSettings])(new FileDataSourceReader(_)),
    source("sql", classOf[SqlSource], classOf[SqlSourceSettings])(new SqlSourceReader(_)),
    source("kafka", classOf[KafkaSource], classOf[KafkaSourceSettings])(new KafkaSourceReader(_)),
    source("fhir-server", classOf[FhirServerSource], classOf[FhirServerSourceSettings])(
      new FhirServerDataSourceReader(_)
    )
  )

  override def sinkProviders: Seq[SinkProvider] = Seq(
    sink("fhir-repository", classOf[FhirRepositorySinkSettings])(s =>
      new FhirRepositoryWriter(s.asInstanceOf[FhirRepositorySinkSettings])
    ),
    sink("file", classOf[FileSystemSinkSettings])(s => new FileSystemWriter(s.asInstanceOf[FileSystemSinkSettings]))
  )

  override def terminologyServiceProviders: Seq[TerminologyServiceProvider] = Seq(
    new TerminologyServiceProvider {
      override val settingsClass: Class[_ <: TerminologyServiceSettings] = classOf[FhirRepositorySinkSettings]
      override def create(settings: TerminologyServiceSettings): IFhirTerminologyService = {
        import io.ignifyr.engine.Execution.actorSystem
        implicit val ec: ExecutionContext = actorSystem.dispatcher
        new TerminologyServiceClient(settings.asInstanceOf[FhirRepositorySinkSettings].createOnFhirClient(actorSystem))
      }
    },
    new TerminologyServiceProvider {
      override val settingsClass: Class[_ <: TerminologyServiceSettings] = classOf[LocalFhirTerminologyServiceSettings]
      override def create(settings: TerminologyServiceSettings): IFhirTerminologyService =
        new LocalTerminologyService(settings.asInstanceOf[LocalFhirTerminologyServiceSettings])
    }
  )

  override def identityServiceProviders: Seq[IdentityServiceProvider] = Seq(
    new IdentityServiceProvider {
      override val settingsClass: Class[_ <: IdentityServiceSettings] = classOf[FhirRepositorySinkSettings]
      override def create(settings: IdentityServiceSettings): IFhirIdentityService = {
        import io.ignifyr.engine.Execution.actorSystem
        implicit val ec: ExecutionContext = actorSystem.dispatcher
        new IdentityServiceClient(settings.asInstanceOf[FhirRepositorySinkSettings].createOnFhirClient(actorSystem))
      }
    }
  )

  override def cliCommands: Seq[CliCommandProvider] = Seq(
    command("help")(new Help()),
    command("load")(new Load()),
    command("extract-redcap-schemas")(new ExtractRedCapSchemas()),
    command("reload")(new Reload()),
    command("run", Seq("execute"))(new Run()),
    command("list")(new ListRunningMappings()),
    command("stop")(new Stop()),
    command("exit", Seq("quit"))(new Exit())
  )

  private def source(
      identifier: String,
      binding: Class[_ <: MappingSourceBinding],
      settings: Class[_ <: MappingJobSourceSettings]
  )(reader: SparkSession => BaseDataSourceReader[_, _]): SourceConnector =
    new SourceConnector {
      override val id: String = identifier
      override val bindingClass: Class[_ <: MappingSourceBinding] = binding
      override val settingsClass: Class[_ <: MappingJobSourceSettings] = settings
      override def createReader(spark: SparkSession): BaseDataSourceReader[_, _] = reader(spark)
    }

  private def sink(identifier: String, settings: Class[_ <: FhirSinkSettings])(
      writer: FhirSinkSettings => BaseFhirWriter
  ): SinkProvider =
    new SinkProvider {
      override val id: String = identifier
      override val settingsClass: Class[_ <: FhirSinkSettings] = settings
      override def createWriter(sinkSettings: FhirSinkSettings): BaseFhirWriter = writer(sinkSettings)
    }

  private def command(commandName: String, commandAliases: Seq[String] = Nil)(mk: => Command): CliCommandProvider =
    new CliCommandProvider {
      override val name: String = commandName
      override val aliases: Seq[String] = commandAliases
      override def create(): Command = mk
    }
}
