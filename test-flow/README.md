# Ignifyr — how to test

Tests are organized into the **standard tiers**:

- **Short (unit)** — fast, no Docker. `test-flow/run-automated-tests.sh --short` (= `mvn test`).
- **Long (integration)** — real services in throwaway Docker containers, plus the edition checks.
  `test-flow/run-automated-tests.sh --long` (= `mvn -B verify -DskipITs=false` + the checks).
- **E2E (live smoke)** — a full running stack; a separate command because it builds images and leaves
  a stack up. `test-flow/run-manual-flow.sh`.

Use **short** for quick feedback, **long** before merging/releasing, **E2E** to smoke-test the packaged runtime.

### The long tier is opt-in

The root pom defines `<skipITs>true</skipITs>`, and **every** `integration-test`-phase execution is
gated on it. So none of the ordinary local build commands start a container:

| Command | Runs |
|---|---|
| `mvn test` | short only |
| `mvn package` | short only |
| `mvn install` | short only — the `integration-test`/`verify` phases execute but skip their suites |
| `mvn verify` | short only |
| `mvn verify -DskipITs=false` | **short + long** |

`-DskipITs=false` is the single switch. CI is the only thing that flips it by default
(`.github/workflows/maven.yml`), which is why opening a PR runs the long tier and building locally
does not.

---

## Prerequisites

- **Java 11 + Maven** — required for any testing.
- **Docker running** — required for the **long** and **E2E** tiers (they start throwaway containers).
- **Windows only:** Hadoop 3.3.x `winutils.exe` + `hadoop.dll`, with `HADOOP_HOME` set (Spark needs the native lib for file access). Not needed on Linux/macOS.
- Run all commands from the repository root.

---

## Short tier — unit tests (fast, no Docker)

```bash
test-flow/run-automated-tests.sh --short     # = mvn test
```

Compiles everything and runs the quick, in-memory unit suites. No containers.

## Long tier — full verification (Docker required)

```bash
test-flow/run-automated-tests.sh --long      # = mvn -B verify -DskipITs=false + the checks
```

Runs the tier gate (`check-test-tiers.sh`), then the unit tests **plus** the integration tests (which
start MongoDB + a FHIR server + Kafka), then the packaged edition-separation checks
(`check-editions.sh`, `check-enforcer-gate.sh`).

## E2E tier — live smoke (separate)

```bash
test-flow/run-manual-flow.sh                 # tool-only stack + behavior checks; --with-web for the UI
```

Builds the server image, stands up a real stack, runs each behavior against it, prints PASS/FAIL, and leaves the stack running (`--down` to remove it).

---

## Command cheat-sheet

| Goal | Command |
|---|---|
| Build only, skip tests | `mvn -DskipTests install` |
| Short (unit, no Docker) | `test-flow/run-automated-tests.sh --short` |
| Long (unit + integration + edition) | `test-flow/run-automated-tests.sh --long` |
| Tier integrity gate (seconds, no JDK) | `test-flow/check-test-tiers.sh` |
| One area only | `test-flow/run-automated-tests.sh --behavior NAME` |
| Edition jars + SPI | `test-flow/check-editions.sh` |
| Edition enforcer gate | `test-flow/check-enforcer-gate.sh` |
| Release readiness (jar contents, licensing) | `test-flow/check-release-ready.sh` (`--release` to make every check fatal) |
| E2E live stack (tool only) | `test-flow/run-manual-flow.sh` |
| E2E + web UI (visual) | `test-flow/run-manual-flow.sh --with-web` (add `--with-efk` for the dashboard) |
| Tear the live stack down | `test-flow/run-manual-flow.sh --down` |

`NAME` for `--behavior`: `streaming` · `scheduling` · `kafka` · `archiving` · `connectors` · `sinks` ·
`endpoints` · `editions`.

---

## What each test covers (plain English)

"Docker" = needs a container to run.

| Test | Tier | What it proves | Docker |
|---|---|---|---|
| `StreamingFolderWatchTest` | long | Drop a file into a watched folder → it turns into FHIR records | yes |
| `KafkaStreamingRedcapTest` | long | REDCap-style records on Kafka → turn into FHIR records (stands in for the REDCap service) | yes |
| `CommunityEditionSeparationSpec` | short | The free edition genuinely does not contain the paid features | no |
| `check-editions.sh` | long | The built free/paid jars contain the right things; the free CLI refuses paid jobs | no* |
| `check-enforcer-gate.sh` | long | Adding a banned (paid) library to a free module makes the build fail | no |
| `check-release-ready.sh` | short | What actually ships: the fat jars credit every bundled dependency's NOTICE and carry Ignifyr's own LICENSE, no copyleft-only library reaches the free edition, and the credential-carrying terminology tool is in neither jar | no° |
| `check-test-tiers.sh` | short | No container-backed suite can hide in the short tier; no module leaves its suites unpinned; no module owns test sources that never run; every integration execution is gated on `skipITs` | no |
| `SchedulingTest` | long | A timer-scheduled job actually fires and writes data | yes |
| `FileStreamInputArchiverTest` | short | Processed input files get archived/deleted as configured | no |
| Connector specs (file / SQL / FHIR-server) | short + long | Each input source reads correctly | some |
| Sink specs (fhir / file / omop) | short | Each output writes correctly | no |
| Server endpoint suites (11 of 14) | short | The REST API (projects, jobs, mappings, terminology) behaves | no |
| `SchemaEndpointTest`, `MappingExecutionEndpointTest`, `FhirDefinitionsEndpointTest` | long | The three endpoint suites that need a real FHIR server | yes |
| `FolderDBInitializerTest` | short | The server starts: the project index is read back, or rebuilt by scanning if it is lost | no |
| Helper suites (`CsvUtil`, `DataFrameUtil`, `SchemaConverter`, `RedCapUtil`, RxNorm client) | short | The lookup tables and file rewrites behind the API, where a wrong answer is silent rather than an error | no |

\* `check-editions.sh` builds the jars itself (no live containers).

° `check-release-ready.sh` also builds the jars itself. Bare it is a per-commit guard; with
`--release` the version and working-tree checks become fatal too. See [RELEASING.md](../RELEASING.md).

---

## The E2E live flow (run-manual-flow.sh)

Tool-only by default. Options:

```bash
test-flow/run-manual-flow.sh                 # tool-only
test-flow/run-manual-flow.sh --only NAME     # one behavior: plugins|batch|archive|streaming|scheduling|kafka|sql
test-flow/run-manual-flow.sh --with-web      # add the web UI (visual), seeded with real projects
test-flow/run-manual-flow.sh --with-efk      # add the Kibana "Executions" dashboard
test-flow/run-manual-flow.sh --down          # stop and remove everything
```

Visual checks you can do with `--with-web` (not automated): import a REDCap data-dictionary into
schemas, open the REDCap page, view the Executions dashboard (`--with-efk`), and browse the mapped
data in the FHIR server.

### SQL and Kafka source jobs (run them from the web UI)

The stack also brings up a **Postgres** container as a SQL data source, seeded on start from
[`data/sql-source-seed.sql`](data/sql-source-seed.sql) (a small `patients` table). Two ways to exercise
the SQL/Kafka sources:

- **Headless behavior:** `run-manual-flow.sh --only sql` maps the Postgres `patients` table to FHIR,
  alongside the existing `--only kafka` (which publishes `data/redcap-patients.ndjson` to the
  `redcap-patients` topic). Both also run as part of a full run.
- **From the UI:** with the stack up, [`create-ui-testflow.sh`](create-ui-testflow.sh) creates a
  `test-flow` project through the REST API — a Patient schema + mapping and two jobs, one SQL (Postgres
  `patients`) and one Kafka (`redcap-patients`) — so both appear in the web UI and can be run and
  observed there, with executions in Kibana. The jobs connect to `jdbc:postgresql://postgres:5432/ignifyr`
  (user/pass `ignifyr`) and topic `redcap-patients`.

Caveat for the Kafka job: the mapping produces deterministic Patient ids, so re-publishing the *same*
records into a live topic makes one micro-batch contain duplicate ids, which the FHIR server rejects
(`Resource identity overlapping`). Start clean with `--down` (the topic is ephemeral) or publish each
record once; distinct records don't collide.

---

## Notes

- **Which tier does a suite belong to?** Its package decides. A short-tier suite lives in its module's
  own package (pinned by `<wildcardSuites>`); a long-tier suite lives in `io.ignifyr.integrationtest`
  (selected by `<membersOnlySuites>` in an `integration-test` execution gated on `${skipITs}`).
  `check-test-tiers.sh` enforces this, so putting a Testcontainers suite in the wrong package fails CI
  rather than quietly slowing everyone's `mvn test` down. It also fails a module that has test sources but
  declares no `scalatest-maven-plugin` at all — those suites compile, look like coverage, and run nowhere.
- **New module with tests?** Declare the plugin and pin `<wildcardSuites>` to the module's own package.
  Every test-bearing module in the reactor does; the short tier is 373 tests across 20 modules.
- Build and test on **JDK 11**
- Integration failures are usually environment, not code: Docker not running, a container still
  pulling, or (Windows) `winutils`/`HADOOP_HOME` not set.
- Generated files (`target/`, `test-flow/web-dist|workspace|watch`) are throwaway — `mvn clean` and
  deleting those folders returns the tree to source-only.
