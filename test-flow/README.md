# Ignifyr — how to test

There are **two ways** to test Ignifyr:

- **Unit tests only** — fast, no Docker. Compiles everything and runs the quick in-memory checks.
- **Everything (extensive)** — the unit tests *plus* integration tests that spin up real services
  (a database, a FHIR server, Kafka) and exercise the tool end to end, plus edition and live-stack checks.

Use unit-only for a quick sanity check; use "everything" before merging or releasing.

---

## Prerequisites

- **Java 11 + Maven** — required for any testing.
- **Docker running** — required only for the *extensive* tests (they start throwaway containers).
- **Windows only:** Hadoop 3.3.x `winutils.exe` + `hadoop.dll`, with `HADOOP_HOME` set (Spark needs the native lib for file access). Not needed on Linux/macOS.
- Run all commands from the repository root.

---

## 1. Unit tests only (fast, no Docker)

```bash
mvn test                                 # compile + all unit suites, no containers
# same thing via the helper:
test-flow/run-automated-tests.sh --unit-only
```

## 2. Everything — extensive tests (Docker required)

```bash
mvn -B verify                            # unit + integration (starts MongoDB + FHIR server + Kafka)
test-flow/check-editions.sh              # edition split (inspects the built jars)
test-flow/check-enforcer-gate.sh         # proves the build blocks enterprise libraries in a free module
test-flow/run-manual-flow.sh             # builds the server image + a live stack, runs each behavior
```

---

## Command cheat-sheet

| Goal | Command                                                                           |
|---|-----------------------------------------------------------------------------------|
| Build only, skip tests | `mvn -DskipTests install`                                                         |
| Unit tests only (no Docker) | `mvn test`                                                                        |
| Everything (unit + integration) | `mvn -B verify`                                                                   |
| One area only | `test-flow/run-automated-tests.sh --behavior NAME`                                |
| Edition separation (jars + SPI) | `test-flow/check-editions.sh`                                                     |
| Edition guardrail (enforcer) | `test-flow/check-enforcer-gate.sh`                                                |
| Live end-to-end (tool only) | `test-flow/run-manual-flow.sh`                                                    |
| Live end-to-end + web UI (visual) | `test-flow/run-manual-flow.sh --with-web` (add `--with-efk` for Kibana dashboard) |
| Tear the live stack down | `test-flow/run-manual-flow.sh --down`                                             |

`NAME` for `--behavior` is one of: `streaming` · `scheduling` · `kafka` · `archiving` · `connectors` ·
`sinks` · `endpoints` · `editions`.

---

## What each test covers (plain English)

Tests added on this branch are marked. "Docker" = needs a container to run.

| Test | What it proves | Docker |
|---|---|---|
| `StreamingFolderWatchTest` | Drop a file into a watched folder → it turns into FHIR records | yes |
| `KafkaStreamingRedcapTest` | REDCap-style records published to Kafka → turn into FHIR records (stands in for the REDCap service) | yes |
| `CommunityEditionSeparationSpec` | The free edition genuinely does not contain the paid features | no |
| `check-editions.sh` | The built free/paid jars contain the right things; the free CLI refuses paid jobs with a clear message | no* |
| `check-enforcer-gate.sh` | Adding a banned library to a free module makes the build fail | no |
| `SchedulingTest` | A timer-scheduled job actually fires and writes data | yes |
| `FileStreamInputArchiverTest` | Processed input files get archived/deleted as configured | no |
| Connector specs (file / SQL / FHIR-server) | Each input source reads correctly | some |
| Sink specs (fhir / file / omop) | Each output writes correctly | no |
| Server endpoint suites | The REST API (projects, jobs, executions) behaves | yes |

\* `check-editions.sh` builds the jars itself (no live containers).

---

## The live "manual flow" (run-manual-flow.sh)

Builds the server image and starts a real stack (MongoDB + FHIR server + Kafka + Ignifyr), then runs
each behavior against it and prints PASS/FAIL. **Tool-only by default — no UI.**

```bash
test-flow/run-manual-flow.sh                 # tool-only
test-flow/run-manual-flow.sh --only NAME     # one behavior: plugins|batch|archive|streaming|scheduling|kafka
test-flow/run-manual-flow.sh --with-web      # add the web UI (visual), seeded with real projects
test-flow/run-manual-flow.sh --with-efk      # add the Kibana "Executions" dashboard
test-flow/run-manual-flow.sh --down          # stop and remove everything
```

Visual checks you can do with `--with-web` (not automated): import a REDCap data-dictionary into
schemas, open the REDCap page, view the Executions dashboard (`--with-efk`), and browse the mapped
data in the FHIR server.

---

## Notes

- On JDK 17+ the build adds Spark's required `--add-opens` automatically; JDK 11 is the target.
- Integration failures are usually environment, not code: Docker not running, a container still
  pulling, or (Windows) `winutils`/`HADOOP_HOME` not set.
- Generated files (`target/`, `test-flow/web-dist|workspace|watch`) are throwaway — `mvn clean` and
  deleting those folders returns the tree to source-only.
