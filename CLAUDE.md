# Ignifyr — agent guide

Ignifyr (formerly **toFHIR**) is a FHIR-first ETL engine: it reads legacy health data
(files, RDBMS, Kafka, REDCap, FHIR servers) and maps it to HL7 FHIR resources. This repository is the
**Community Edition**: the engine, the file/SQL connectors, the FHIR and file sinks, and the standalone
CLI/batch tool. The REST server, streaming, scheduling and the advanced connectors are the private
Enterprise Edition (see [Modules](#modules)).

**Stack:** Scala 2.13.16 · JDK 11 · Apache Spark 3.5.4 · Typesafe Config (HOCON) · Maven
(multi-module). License Apache-2.0. Maintained by SRDC, commercially supported by Pontegra.

> Deep usage & configuration docs: [README.md](README.md). Cutting a release:
> [RELEASING.md](RELEASING.md) — a release is a tag, the library modules deployed to SRDC Nexus, the
> CLI fat jar and its Docker image.

## Naming: the toFHIR → Ignifyr rename is done

Ignifyr was formerly **toFHIR**; the wholesale rename has landed. Packages
(`io.ignifyr.*`), Maven coordinates (`io.ignifyr:ignifyr-*`), HOCON keys (`ignifyr.*`,
`ignifyr-redcap`), Docker tags (`srdc/ignifyr-*`), and the `ignifyr-db` folder all use the
new name. The sibling projects are renamed too (the `srdc/ignifyr-web` UI image, the
enterprise `ignifyr-redcap-service`). The legacy `tofhir` name survives **only** in the
README's SwaggerHub docs link (until the API docs are republished under Ignifyr), git
history, the `tofhir-db/` line in `.gitignore`, and the "formerly toFHIR" mentions
(including the README's pointers to `srdc/tofhir-web` and the retired
[tofhir-redcap](https://github.com/srdc/tofhir-redcap) repo). Don't reintroduce `tofhir` in
new code; `mapToFhir`-style identifiers mean "map *to FHIR*" and are correct as-is.

## Modules

This repository is the public **Community Edition** (Apache-2.0, the CLI batch engine). The private
**Enterprise Edition** (advanced connectors, streaming/scheduling, the REST server) lives in its own
repository and builds on this one's artifacts, published to SRDC Nexus. The design rule: **moving a
feature between editions is a one-folder module move (between the two repositories) with zero engine
code changes.**

Every plugin module plugs in through the `IgnifyrExtension` ServiceLoader SPI
(`io.ignifyr.engine.spi`) — one implementation and one `META-INF/services` entry per jar. Its full
contribution surface is `sourceConnectors`, `sinkProviders`, `terminologyServiceProviders`,
`identityServiceProviders`, `cliCommands`, `sourceFailureDescriptors`, `schemaInferrers`,
`streamingProvider`, `schedulerProvider`, `sparkConfContributions`, `extraCapabilities`, plus
`initialize(config)` (scoped to `ignifyr.extensions.<id>`; it must **not** touch
`IgnifyrConfig.sparkSession` — registry load runs while the session is being built). The engine's
`ExtensionRegistry` indexes these by settings/binding **class**, fail-fast on duplicate keys
(`streamingProvider`/`schedulerProvider` are single-capability `Option`s) — the engine never names a
connector, sink, or format directly. `ExtensionRegistry.init()` force-materializes every registry that
can reject its input, **including `streaming` and `scheduler`**, so a second copy of a capability
module fails at startup rather than at first job launch.

`sparkConfContributions` is merged in three layers, lowest first: engine defaults → module
contributions → the user's `spark { }` block. The user's block wins for a single-valued key, but for a
key Spark parses as a comma-separated registration list (`IgnifyrConfig.additiveSparkConfKeys` —
`spark.sql.extensions`, `spark.plugins`, `spark.jars*`, the listener keys) **all layers are joined and
deduplicated**, so setting `spark.sql.extensions` yourself cannot silently drop the Delta session
extension `ignifyr-format-delta` registers. Two deliberate exclusions: `*.extraClassPath` (path-separator
delimited, not commas) and `spark.sql.catalog.spark_catalog` (a genuine scalar a user may override).
Caveat: the *cross-module* merge in `ExtensionRegistry` is stricter — only `spark.sql.extensions` is
additive there, so two modules claiming any other key still fail fast. **The engine ships no concrete I/O:** `CoreExtension`
contributes only the built-in CLI commands and the CSV-backed `LocalTerminologyService`. A missing
plugin surfaces through `ExtensionHints`, which maps a model class to the Maven coordinates to
install; a maven-enforcer `bannedDependencies` gate (`ban-enterprise-deps`, opted into by 8 community
modules) keeps Kafka, cron4j, Delta, DB2 JCC, and Logstash/Fluentd out of them.

Dependency direction: `ignifyr-common` ← `ignifyr-engine` ← the connector/sink plugin modules (each
depends on the engine and is discovered at runtime). `ignifyr-cli` is the community **distribution** — it
shades the engine plus the community plugin modules, so *the edition of a feature is one line in this
POM* (and one in the enterprise `ignifyr-server` POM). The enterprise `ignifyr-format-*` modules depend on
the community module whose sub-SPI they extend (`format-json` → `connector-file`, `format-delta` →
`sink-file`), which is why those sub-SPIs are API. `ignifyr-testkit` is consumed at test scope by both
editions' suites.

Artifact naming: everything is `ignifyr-*_2.13` **except** `ignifyr-common` (and the enterprise
`ignifyr-server-common`), which carry no Scala suffix.

**Community** (`io.ignifyr`, published to SRDC Nexus; bundled into `ignifyr-cli`):

| Module | Purpose |
|---|---|
| [`ignifyr-engine`](ignifyr-engine/CLAUDE.md) | Core mapping/transformation engine, CLI/batch entrypoint, and the extension SPI |
| [`ignifyr-common`](ignifyr-common/CLAUDE.md) | Spark-free helper layer: version reader, onFHIR schema→StructureDefinition conversion, exception flattening, the `cst:` FHIRPath library. No model classes |
| `ignifyr-connector-sql` | SQL/JDBC source connector (ships the PostgreSQL driver) + the JDBC-metadata schema inferrer |
| [`ignifyr-connector-file`](ignifyr-connector-file/CLAUDE.md) | File-system source; owns a pluggable source-format sub-SPI (`FileSourceFormat`, its own `FileFormatRegistry`/ServiceLoader) shipping csv/tsv/parquet |
| [`ignifyr-sink-fhir`](ignifyr-sink-fhir/CLAUDE.md) | FHIR-repository sink writer (the flagship output) + the FHIR-server-backed terminology/identity service providers |
| [`ignifyr-sink-file`](ignifyr-sink-file/CLAUDE.md) | File-system sink; owns the pluggable sink-format sub-SPI (`FileSinkFormat`, its own `FileSinkFormatRegistry`/ServiceLoader) shipping ndjson/csv/parquet |
| [`ignifyr-cli`](ignifyr-cli/CLAUDE.md) | Community standalone fat-jar assembly (Main-Class `io.ignifyr.engine.Boot`); no source of its own |
| [`ignifyr-testkit`](ignifyr-testkit/CLAUDE.md) | Shared test harness (`IgnifyrTestSpec`, `OnFhirTestContainer`, fixtures); test-only, never shipped |

**Enterprise** (private repository on `gitlab.srdc.com.tr`, groupId `com.pontegra.ignifyr`, bundled
into `ignifyr-server`): server, server-common, redcap, connector-kafka, connector-fhir-server,
format-json, format-delta, sink-omop, runtime-streaming, runtime-scheduling, observability,
terminology-tools, rxnorm. Its root pom's **parent is this repository's root pom**, resolved from SRDC
Nexus, so it inherits the build platform and the `io.ignifyr` dependency management. Two consequences
for changes here:
- The root pom is a published contract. Its `pluginManagement`/`dependencyManagement` are what the
  enterprise build runs on — flatten runs in `resolveCiFriendliesOnly` mode precisely so the deployed
  root pom keeps them, and the managed `io.ignifyr` versions are `${revision}` (baked to a literal on
  deploy) rather than `${project.version}` (which would re-resolve to the *enterprise* version there).
- Anything an enterprise module compiles against — the SPI, the settings models, `ignifyr-testkit` and
  its fixtures — is API. Breaking it breaks the enterprise build at its next snapshot.

Modules linked above have their own `CLAUDE.md`; `connector-sql` is deliberately without one — its
rationale is in its pom's header comment. One caveat worth stating: its inferrer returns `None` for a
`preprocessSql` source so the engine falls back to Spark-read inference.

## Build / test / run

Run everything from the repo root with Maven.

**Build**
- `mvn -DskipTests install` — compile + package all modules.
- Standalone fat jar: `ignifyr-cli/target/ignifyr-engine-standalone.jar`.
- `mvn -DskipTests deploy` publishes the library modules (not `ignifyr-cli`) to SRDC Nexus. CI does it
  for `main`'s snapshot; releases are deployed by hand ([RELEASING.md](RELEASING.md)).

**Test** — ScalaTest via the `scalatest-maven-plugin` (surefire is disabled). Tests are split into
**tiers**; the canonical guide is **[test-flow/README.md](test-flow/README.md)** — read it rather than
re-deriving the commands, and keep it as the single source of truth if you change the test setup.

- `mvn test` — the **short (unit)** tier: fast and **Docker-free across the whole reactor** (176 tests
  over 8 modules, verified 2026-09-24).
  Every module pins its own `wildcardSuites` (the engine's is `io.ignifyr.test`, a plugin module's is its
  own package, e.g. `io.ignifyr.sink.file`).
- `mvn -B verify -DskipITs=false` — short **plus** the **long (integration)** tier.
  ⚠️ Requires **Docker running** (TestContainers: MongoDB, onFHIR r5).
- ⚠️ **The long tier is opt-in.** The root pom sets `<skipITs>true</skipITs>` and every
  `integration-test` execution is gated on it, so plain `mvn test`/`package`/`install`/**`verify`** all
  stay short and container-free. `-DskipITs=false` is the only switch. Modules owning long-tier suites
  (all in package `io.ignifyr.integrationtest`): `connector-file`, `connector-sql`.
- `test-flow/run-automated-tests.sh --short | --long` wraps the two tiers and additionally runs the
  edition checks.
- One module: `mvn test -pl ignifyr-engine`.
- One suite: `mvn test -pl ignifyr-engine -Dsuffixes='.*ListPluginsTest'`. Use `-Dsuffixes` (a regex on
  the suite name) — `-DwildcardSuites` and `-Dsuites` are **silently ignored**, because an explicit
  value in the module POM beats the command-line property.
- **Every module with test sources runs them under Maven**; the tier gate has an invariant for the gap
  (test sources but no scalatest plugin) so it cannot reopen.
- **Run `mvn test` and report the result before claiming a change works.**

**Format** — scalafmt, config in [.scalafmt.conf](.scalafmt.conf).
- `mvn scalafmt:format` — format all Scala. Run before committing. Not bound to the build,
  so it never runs automatically during compile/verify/CI.

**Run**
- Engine CLI (default): `java -jar ignifyr-cli/target/ignifyr-engine-standalone.jar`
- Engine batch: `java -jar ignifyr-cli/target/ignifyr-engine-standalone.jar run --job <job.json>`
- Add `-Dconfig.file=<ignifyr.conf>` to override the bundled config.
- Docker image: `docker/engine/build.sh` → `srdc/ignifyr-engine`. The REST server, its Docker image and
  the full local stack (web UI, EFK) are enterprise and live in the enterprise repository.

## Testing model

- ScalaTest 3.2 styles: `AnyFlatSpec` (unit), `AnyWordSpec` (endpoints), `AsyncFlatSpec`.
  Mocking: mockito-scala. In-memory DB: H2.
- The shared harness lives in **`ignifyr-testkit`**, in `src/main` (not a test-jar):
  [`IgnifyrTestSpec`](ignifyr-testkit/src/main/scala/io/ignifyr/IgnifyrTestSpec.scala) provides a
  `SparkSession`, mapping/schema repositories, a context loader, and a `RunningJobRegistry`;
  `OnFhirTestContainer` provides the onFHIR + MongoDB containers; fixtures live on its classpath.
  Depend on it at **test** scope — that one dependency also brings scalatest/mockito/H2/Testcontainers,
  which the testkit declares at compile scope on purpose.
  - A consumer of `IgnifyrTestSpec` **must** set `ignifyr.mappings.repository.folder-path` and
    `ignifyr.mappings.schemas.repository.folder-path` in its own `src/test/resources/application.conf` —
    the spec reads them from config rather than hardcoding the fixture roots. This is the usual
    first-failure for a new module.
  - The **engine's own** suites do not use the harness (the engine must never depend on the testkit —
    that would cycle); all thirteen are self-contained `AnyFlatSpec`s. Keep new engine suites that way:
    one reaching for a fixture folder or a connector belongs in the module that supplies the I/O.
  - Resolve a shared fixture folder with `IgnifyrTestSpec.testResourceFolderPath`, never
    `Paths.get(getClass.getResource(...).toURI)` — the reactor serves testkit fixtures from the jar.
- The enterprise server's endpoint suites (`BaseEndpointTest`) live in the enterprise repository and consume
  this testkit from SRDC Nexus — a fixture change here reaches them at the next snapshot deploy.
- **A suite that writes to the onFHIR container must read the resources back.** Asserting that the
  teardown `delete` answered 200 proves nothing: it answers 200 whether or not anything was ever written,
  which is how two R4-shaped fixtures went unnoticed against the r5 container until 2026-08-07.
- **A suite's tier is decided by its package, and that is enforced, not conventional:**
  short = the module's own package pinned by `<wildcardSuites>`; long = `io.ignifyr.integrationtest`
  selected by `<membersOnlySuites>` in an `integration-test` execution gated on `${skipITs}`.
  `test-flow/check-test-tiers.sh` (seconds, no Maven, no Docker; a CI job of its own) fails the build if
  a container-backed suite sits in a short-tier package, if a module declares a **bare**
  `scalatest-maven-plugin`, if a module owns test sources but **no** `scalatest-maven-plugin` (invariant
  2b — such suites compile, look like coverage, and never run), or if integration suites and integration
  executions disagree. So put a new Testcontainers suite in `io.ignifyr.integrationtest` — anywhere else
  and CI rejects it rather than quietly slowing everyone's `mvn test`.
- Integration tests need Docker; treat live-network failures as environment issues, not regressions.
- The root pom's scalatest `argLine` adds the Spark `--add-opens` module flags (with
  `-XX:+IgnoreUnrecognizedVMOptions`, so it stays valid on the JDK 11 target). That is what keeps
  Spark-backed suites from aborting on JDK 17+ with `cannot access class sun.nio.ch.DirectBuffer` —
  don't drop it. **Build and test on JDK 11** regardless; that is what CI uses.

## Commit & PR conventions (SRDC semantic commits)

Format: `<emoji> <type>(<scope>): <subject>. <issue-ref>`

| type | emoji | for |
|---|---|---|
| feat | ✨ `:sparkles:` | new feature |
| fix | 🐛 `:bug:` | bug fix |
| docs | 📝 `:memo:` | documentation |
| refactor | ♻️ `:recycle:` | refactor (no behavior change) |
| build | 👷 `:construction_worker:` | build system / dependencies |
| test | ✅ `:white_check_mark:` | tests |
| ci | 💚 `:green_heart:` | CI/CD |
| style | 🎨 `:art:` | formatting (no logic change) |
| chore | 🔧 `:wrench:` | maintenance |
| perf | ⚡ `:zap:` | performance |

- Subject in the imperative; `scope` is optional. Optional issue auto-close: `Fixes #N` / `Closes #N`.
- Examples: `:sparkles: feat(engine): add Avro file source reader` ·
  `:bug: fix(server): return 404 for missing mapping. Fixes #312`
- Before committing: `mvn scalafmt:format` then `mvn test`. PRs target `main`; CI (short tier, then the
  long tier on the PR) must be green.
- Convention source: [SRDC wiki — semantic commit messages](https://github.com/srdc/wiki/blob/master/semantic-commit-messages-with-emojis.md).

## Configuration

- HOCON (Typesafe Config). Engine reference config:
  [ignifyr-engine/src/main/resources/application.conf](ignifyr-engine/src/main/resources/application.conf)
  — settings live under `ignifyr { … }`: `context-path`, `mappings` (which **nests** `schemas` and
  `contexts` — there is no top-level `ignifyr.schemas`), `mapping-jobs`, `terminology-systems`,
  `archiving`, `fhir-server-writer`, `db-path`, and the commented-out `functionLibraries` block (where
  the enterprise `ignifyr-rxnorm`'s `rxn:` and `ignifyr-common`'s `cst:` FHIRPath libraries are attached by class name).
  Plus `spark { … }` and `akka { … }`.
- `ignifyr.extensions.<id>` is the per-module SPI subtree handed to `IgnifyrExtension.initialize`. It is
  absent from the reference config by design — an extension gets an empty `Config` when its block is
  missing.
- The enterprise server adds `webserver`, `fhir`, and `ignifyr-redcap` blocks, and *activates* the
  `functionLibraries` entries the engine leaves commented out.
- Override at runtime with `-Dconfig.file=<path>` or single keys `-D<key>=<value>`.
- Test config: `*/src/test/resources/application.conf`. A module whose suites use `IgnifyrTestSpec` must
  point `ignifyr.mappings.repository.folder-path` / `…mappings.schemas.repository.folder-path` at the
  testkit fixtures there.

## Environment notes

- Primary development is on **Windows / PowerShell**; all Maven commands above are cross-platform.
- **Docker Desktop** must be running for `mvn -B verify` / integration tests.
- Upstream dependencies are all releases: onfhir-libs 4.0.0 from **Maven Central**;
  onfhir-definition-microservices 2.0.0 and spark-on-fhir 2.0.0 (`io.onfhir.spark`, a reactor since
  2.0.0 — Ignifyr consumes `spark-on-fhir-connector-api`) from **SRDC Nexus releases**, the one
  `<repository>` in [pom.xml](pom.xml). No snapshot repository is declared; Ignifyr's own
  `${revision}` is the only `-SNAPSHOT` in the build.
