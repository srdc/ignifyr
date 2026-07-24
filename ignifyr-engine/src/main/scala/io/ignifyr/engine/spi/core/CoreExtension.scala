package io.ignifyr.engine.spi.core

import io.onfhir.api.service.IFhirTerminologyService
import io.ignifyr.engine.cli.command.{Command, Exit, Help, ListPlugins, ListRunningMappings, Load, Reload, Run, Stop}
import io.ignifyr.engine.mapping.service.LocalTerminologyService
import io.ignifyr.engine.model._
import io.ignifyr.engine.spi._

/**
 * The engine's own [[IgnifyrExtension]], registered through the same ServiceLoader mechanism as
 * every other module (no special-casing in the registries). With every concrete source connector
 * and sink extracted into its own module — including the FHIR-repository sink
 * (`ignifyr-sink-fhir`) — the core contributes only what is intrinsic to the engine: the built-in
 * CLI commands and the local (CSV-backed) terminology service. The engine ships no concrete I/O.
 */
class CoreExtension extends IgnifyrExtension {

  override val id: String = "core"

  override def terminologyServiceProviders: Seq[TerminologyServiceProvider] = Seq(
    new TerminologyServiceProvider {
      override val settingsClass: Class[_ <: TerminologyServiceSettings] = classOf[LocalFhirTerminologyServiceSettings]
      override def create(settings: TerminologyServiceSettings): IFhirTerminologyService =
        new LocalTerminologyService(settings.asInstanceOf[LocalFhirTerminologyServiceSettings])
    }
  )

  override def cliCommands: Seq[CliCommandProvider] = Seq(
    command("help")(new Help()),
    command("load")(new Load()),
    command("reload")(new Reload()),
    command("run", Seq("execute"))(new Run()),
    command("list")(new ListRunningMappings()),
    command("list-plugins")(new ListPlugins()),
    command("stop")(new Stop()),
    command("exit", Seq("quit"))(new Exit())
  )

  private def command(commandName: String, commandAliases: Seq[String] = Nil)(mk: => Command): CliCommandProvider =
    new CliCommandProvider {
      override val name: String = commandName
      override val aliases: Seq[String] = commandAliases
      override def create(): Command = mk
    }
}
