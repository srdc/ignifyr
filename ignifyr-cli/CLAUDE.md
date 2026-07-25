# ignifyr-cli — the community distribution

**This module has no source code.** It is a pom-only assembly: `src/` does not exist, so don't go looking
for a `CliExtension` or a `Main` here. The CLI classes themselves (`Boot`, `CommandLineInterface`,
`cli/command/*`) live in `ignifyr-engine`.

What it does: shade `ignifyr-engine` plus the community plugin modules into
**`target/ignifyr-engine-standalone.jar`** — note the jar is named after the *engine*, not after this
module — with Main-Class `io.ignifyr.engine.Boot`.

## Why it is a module
1. **The assembly cannot live in the engine.** The plugin modules depend on the engine, so the engine
   cannot depend on them without a reactor cycle; the jar that bundles both must be built downstream of
   all of them.
2. **Its dependency list *is* the Community edition.** Moving a feature between editions reduces to
   moving one line between this pom and `ignifyr-server/pom.xml`. Today it declares:
   `ignifyr-engine`, `ignifyr-connector-sql`, `ignifyr-connector-file`, `ignifyr-sink-fhir`,
   `ignifyr-sink-file` (`ignifyr-common` arrives transitively via the engine).
3. **It is where the edition boundary is *proved*.** It opts into the root `ban-enterprise-deps`
   maven-enforcer execution with `searchTransitive=true`, so simply building this module asserts that
   nothing reachable from the community jar drags in Kafka, cron4j, Delta, the DB2 JCC driver, or
   Logstash/Fluentd.

## The load-bearing shade detail
The shade plugin's **`ServicesResourceTransformer` is not optional** — it merges the bundled modules'
`META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension` files instead of letting one overwrite the
others. Without it the fat jar would expose exactly one extension and `ExtensionRegistry` would silently
lose every other plugin. If you add a module here and its contributions vanish at runtime, suspect that
transformer first.

## Smoke checks
```bash
java -jar ignifyr-cli/target/ignifyr-engine-standalone.jar list-plugins
```
should print exactly `connector-file`, `connector-sql`, `core`, `sink-fhir`, `sink-file` — where `core` is
CLI commands plus the local terminology service only, because the engine ships no concrete I/O. A job
naming an enterprise source/sink should still *parse* and then fail with an "install the `…` module"
message; that negative path is as much a part of the contract as the happy path.
