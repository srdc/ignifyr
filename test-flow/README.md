# Ignifyr Community — how to test

Tests are organized into the **standard tiers**:

- **Short (unit)** — fast, no Docker. `test-flow/run-automated-tests.sh --short` (= `mvn test`).
- **Long (integration)** — real services in throwaway Docker containers, plus the edition checks.
  `test-flow/run-automated-tests.sh --long` (= `mvn -B verify -DskipITs=false` + the checks).

Use **short** for quick feedback, **long** before merging/releasing. The live end-to-end stack (server,
web UI, Kafka, streaming, scheduling) belongs to the enterprise edition and is tested in its repository.

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
- **Docker running** — required for the **long** tier (it starts throwaway containers).
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
start MongoDB + a FHIR server), then the packaged edition-separation checks
(`check-editions.sh`, `check-enforcer-gate.sh`).

---

## Command cheat-sheet

| Goal | Command |
|---|---|
| Build only, skip tests | `mvn -DskipTests install` |
| Short (unit, no Docker) | `test-flow/run-automated-tests.sh --short` |
| Long (unit + integration + edition) | `test-flow/run-automated-tests.sh --long` |
| Tier integrity gate (seconds, no JDK) | `test-flow/check-test-tiers.sh` |
| One area only | `test-flow/run-automated-tests.sh --behavior NAME` |
| Edition jar + SPI + CLI refusals | `test-flow/check-editions.sh` |
| Edition enforcer gate | `test-flow/check-enforcer-gate.sh` |
| Release readiness (jar contents, licensing) | `test-flow/check-release-ready.sh` (`--release` to make every check fatal) |

`NAME` for `--behavior`: `archiving` · `connectors` · `sinks` · `editions`.

---

## What each test covers (plain English)

"Docker" = needs a container to run.

| Test | Tier | What it proves | Docker |
|---|---|---|---|
| `CommunityEditionSeparationSpec` | short | The free edition genuinely does not contain the paid features | no |
| `check-editions.sh` | long | The built free jar contains no paid code or library, its plugin list is exactly the free one, and the free CLI refuses paid jobs | no* |
| `check-enforcer-gate.sh` | long | Adding a banned (paid) library to a free module makes the build fail | no |
| `check-release-ready.sh` | short | What actually ships: the fat jar credits every bundled dependency's NOTICE and carries Ignifyr's own LICENSE, and no copyleft-only library reaches the free edition | no° |
| `check-test-tiers.sh` | short | No container-backed suite can hide in the short tier; no module leaves its suites unpinned; no module owns test sources that never run; every integration execution is gated on `skipITs` | no |
| `FileStreamInputArchiverTest` | short | Processed input files get archived/deleted as configured | no |
| Connector specs (file / SQL) | short + long | Each input source reads correctly | some |
| Sink specs (fhir / file) | short | Each output writes correctly | no |
| Helper suites (`DataFrameUtil`, `SchemaConverter`, …) | short | The lookup tables and file rewrites behind the API, where a wrong answer is silent rather than an error | no |

\* `check-editions.sh` builds the jar itself (no live containers).

° `check-release-ready.sh` also builds the jar itself. Bare it is a per-commit guard; with
`--release` the version and working-tree checks become fatal too. See [RELEASING.md](../RELEASING.md).

---

## Notes

- **Which tier does a suite belong to?** Its package decides. A short-tier suite lives in its module's
  own package (pinned by `<wildcardSuites>`); a long-tier suite lives in `io.ignifyr.integrationtest`
  (selected by `<membersOnlySuites>` in an `integration-test` execution gated on `${skipITs}`).
  `check-test-tiers.sh` enforces this, so putting a Testcontainers suite in the wrong package fails CI
  rather than quietly slowing everyone's `mvn test` down. It also fails a module that has test sources but
  declares no `scalatest-maven-plugin` at all — those suites compile, look like coverage, and run nowhere.
- **New module with tests?** Declare the plugin and pin `<wildcardSuites>` to the module's own package.
  Every test-bearing module in the reactor does; the short tier is 176 tests across 8 modules.
- Build and test on **JDK 11**
- Integration failures are usually environment, not code: Docker not running, a container still
  pulling, or (Windows) `winutils`/`HADOOP_HOME` not set.
- Generated files (`target/`) are throwaway — `mvn clean` returns the tree to source-only.
