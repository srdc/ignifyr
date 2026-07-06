# AGENTS.md

Ignifyr (formerly **toFHIR**) — a FHIR-first ETL engine. Scala 2.13 · JDK 11 · Spark · Akka-HTTP ·
Maven multi-module.

Full agent/contributor guidance lives in **[CLAUDE.md](CLAUDE.md)** (root) and per-module `CLAUDE.md`
files (`ignifyr-engine/`, `ignifyr-server/`, `ignifyr-server-common/`, `ignifyr-common/`, `ignifyr-rxnorm/`).
Read those first. The essentials:

1. **Naming.** The toFHIR → Ignifyr rename is complete: packages (`io.ignifyr.*`), modules
   (`ignifyr-*`), config keys, and Docker tags all use the new name. The legacy `tofhir` name remains
   only in references to not-yet-renamed sibling artifacts (`srdc/tofhir-web` image, `tofhir-redcap`
   service) — don't reintroduce it in new code.
2. **Verify with Maven.** `mvn test` for unit tests (fast, no Docker); `mvn -B verify` for the full build
   incl. integration tests — which **need Docker running** (MongoDB/Kafka/onFHIR via TestContainers).
   Report test results before claiming a change works.
3. **Commit format (SRDC semantic commits):** `<emoji> <type>(<scope>): <subject>` — `:sparkles:` feat,
   `:bug:` fix, `:memo:` docs, `:recycle:` refactor, `:construction_worker:` build, `:white_check_mark:`
   test, `:green_heart:` ci, `:art:` style, `:wrench:` chore, `:zap:` perf. Imperative subject; optional
   `Fixes #N`.
4. **Format before committing:** `mvn scalafmt:format`.

PRs target `main`; CI runs `mvn -B verify`.
