package io.ignifyr.test.engine.cli

import io.ignifyr.engine.cli.command.ListPlugins
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayOutputStream, PrintStream}

/**
 * Verifies the `list-plugins` command renders the installed extensions and their contributions. The
 * engine's own test classpath ships only the core extension (connectors/formats live in downstream
 * modules, which the engine cannot depend on without a reactor cycle), so this asserts the core
 * extension, its local terminology service, and that `list-plugins` registers itself as a CLI command.
 * The richer community/enterprise provider set is asserted where those modules are on the classpath
 * (the connector registration specs and the assembled-CLI smoke). `render()` reads only the
 * ExtensionRegistry, so no engine or workspace is needed.
 */
class ListPluginsTest extends AnyFlatSpec with Matchers {

  "The list-plugins command" should "render the core extension and its contributions" in {
    val buffer = new ByteArrayOutputStream()
    Console.withOut(new PrintStream(buffer, true, "UTF-8")) {
      new ListPlugins().render()
    }
    val output = buffer.toString("UTF-8")

    output should include("core")
    output should include("LocalFhirTerminologyServiceSettings")
    // list-plugins is itself a core-registered command, so it must appear in the rendered command set.
    output should include("list-plugins")
  }
}
