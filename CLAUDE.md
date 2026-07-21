# Ignifyr — agent guide

Ignifyr (formerly **toFHIR**) is a FHIR-first ETL engine: it reads legacy health data
(files, RDBMS, Kafka, REDCap, FHIR servers) and maps it to HL7 FHIR resources. It runs
as a library, a standalone CLI/batch tool, or a REST server.

**Stack:** Scala 2.13.16 · JDK 11 · Apache Spark 3.5.4 · Akka-HTTP 10.5.3 · Typesafe
Config (HOCON) · Maven (multi-module). License Apache-2.0. Maintained by SRDC,
commercially supported by Pontegra.

> Deep usage & configuration docs: [README.md](README.md). REST API contract:
> [ignifyr-server/api.yaml](ignifyr-server/api.yaml).

## Naming: the toFHIR → Ignifyr rename is done

Ignifyr was formerly **toFHIR**; the wholesale rename has landed. Packages
(`io.ignifyr.*`), Maven coordinates (`io.ignifyr:ignifyr-*`), HOCON keys (`ignifyr.*`,
`ignifyr-redcap`), Docker tags (`srdc/ignifyr-*`), and the `ignifyr-db` folder all use the
new name. The legacy `tofhir` name survives **only** in references to sibling projects
that haven't renamed yet — the `srdc/tofhir-web` Docker image, the
[tofhir-redcap](https://github.com/srdc/tofhir-redcap) service (endpoint URL value and
repo links), the SwaggerHub docs link — plus git history and the "formerly toFHIR"
mentions. Don't reintroduce `tofhir` in new code; `mapToFhir`-style identifiers mean
"map *to FHIR*" and are correct as-is.

## Modules

Dependency direction: `ignifyr-common` ← `ignifyr-engine` ← `ignifyr-server`;
`ignifyr-server-common` ← `ignifyr-server`; `ignifyr-rxnorm` is standalone.

| Module | Purpose | Detail |
|---|---|---|
| [`ignifyr-engine`](ignifyr-engine/CLAUDE.md) | Core mapping/transformation engine + CLI/batch entrypoint | mapping execution, source readers, sinks, scheduling |
| [`ignifyr-server`](ignifyr-server/CLAUDE.md) | Akka-HTTP REST API to manage projects/jobs | Endpoint → Service → Repository |
| [`ignifyr-server-common`](ignifyr-server-common/CLAUDE.md) | Shared web-server config + CORS/error interceptors | used by the server |
| [`ignifyr-common`](ignifyr-common/CLAUDE.md) | Shared models, version, custom mapping fns, exception utils | used by every module |
| [`ignifyr-rxnorm`](ignifyr-rxnorm/CLAUDE.md) | RxNorm API client + FHIRPath terminology functions | standalone |

## Build / test / run

Run everything from the repo root with Maven.

**Build**
- `mvn -DskipTests install` — compile + package all modules.
- Standalone fat jars: `ignifyr-cli/target/ignifyr-engine-standalone.jar`,
  `ignifyr-server/target/ignifyr-server-standalone.jar`.

**Test** — ScalaTest via the `scalatest-maven-plugin` (surefire is disabled).
- `mvn test` — **unit tests** (suites under `io.ignifyr.test`). Fast loop, no Docker.
- `mvn -B verify` — full build **incl. integration tests** (`io.ignifyr.integrationtest`).
  ⚠️ Requires **Docker running** (TestContainers spins up MongoDB, Kafka, onFHIR r5).
  This is exactly what CI runs.
- One module: `mvn test -pl ignifyr-engine`.
- One suite: `mvn test -pl ignifyr-engine -DwildcardSuites=io.ignifyr.test.engine.data.read.FileDataSourceReaderTest`.
- **Run `mvn test` and report the result before claiming a change works.**

**Format** — scalafmt, config in [.scalafmt.conf](.scalafmt.conf).
- `mvn scalafmt:format` — format all Scala. Run before committing. Not bound to the build,
  so it never runs automatically during compile/verify/CI.

**Run**
- Engine CLI (default): `java -jar ignifyr-cli/target/ignifyr-engine-standalone.jar`
- Engine batch: `java -jar ignifyr-cli/target/ignifyr-engine-standalone.jar run --job <job.json>`
- Server: `java -Dconfig.file=<ignifyr.conf> -jar ignifyr-server/target/ignifyr-server-standalone.jar`
  → REST at `http://localhost:8085/ignifyr`.
- Add `-Dconfig.file=<ignifyr.conf>` to either to override the bundled config.
- Full local stack (server + web UI + Elasticsearch/Fluentd/Kibana):
  `docker compose -f docker/docker-compose.yml up` (needs the external `onfhir-network`; see [docker/](docker/)).

## Testing model

- ScalaTest 3.2 styles: `AnyFlatSpec` (unit), `AnyWordSpec` (endpoints), `AsyncFlatSpec`.
  Mocking: mockito-scala. In-memory DB: H2.
- Engine unit tests extend the shared `IgnifyrTestSpec` trait
  ([ignifyr-engine/src/test/scala/io/ignifyr/IgnifyrTestSpec.scala](ignifyr-engine/src/test/scala/io/ignifyr/IgnifyrTestSpec.scala))
  — provides a `SparkSession`, mapping/schema repositories, a context loader, and a `RunningJobRegistry`.
- Server endpoint tests extend `BaseEndpointTest`
  ([ignifyr-server/src/test/scala/io/ignifyr/server/BaseEndpointTest.scala](ignifyr-server/src/test/scala/io/ignifyr/server/BaseEndpointTest.scala))
  — boots the Akka-HTTP route via `ScalatestRouteTest` with a MongoDB TestContainer.
- The unit (`io.ignifyr.test`) vs integration (`io.ignifyr.integrationtest`) split is configured
  per module in the `scalatest-maven-plugin` (see `ignifyr-engine/pom.xml`). Integration tests need
  Docker; treat live-network failures (e.g. the RxNorm API) as environment issues, not regressions.

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
- Before committing: `mvn scalafmt:format` then `mvn test`. PRs target `main`; CI (`mvn -B verify`) must be green.
- Convention source: [SRDC wiki — semantic commit messages](https://github.com/srdc/wiki/blob/master/semantic-commit-messages-with-emojis.md).

## Configuration

- HOCON (Typesafe Config). Engine reference config:
  [ignifyr-engine/src/main/resources/application.conf](ignifyr-engine/src/main/resources/application.conf)
  — settings live under `ignifyr { … }` (`mappings`, `mapping-jobs`, `schemas`,
  `terminology-systems`, `archiving`, `fhir-server-writer`, `db-path`) plus `spark { … }` and `akka { … }`.
- The server adds `webserver`, `fhir`, and `ignifyr-redcap` blocks.
- Override at runtime with `-Dconfig.file=<path>` or single keys `-D<key>=<value>`.
- Test config: `*/src/test/resources/application.conf`.

## Environment notes

- Primary development is on **Windows / PowerShell**; all Maven commands above are cross-platform.
- **Docker Desktop** must be running for `mvn -B verify` / integration tests.
- onFHIR & spark-on-fhir SNAPSHOT dependencies resolve from Central snapshots + SRDC Nexus
  (see `<repositories>` in [pom.xml](pom.xml)).
