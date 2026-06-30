# AGENTS.md

Ignifyr (formerly **toFHIR**) — a FHIR-first ETL engine. Scala 2.13 · JDK 11 · Spark · Akka-HTTP ·
Maven multi-module.

Full agent/contributor guidance lives in **[CLAUDE.md](CLAUDE.md)** (root) and per-module `CLAUDE.md`
files (`tofhir-engine/`, `tofhir-server/`, `tofhir-server-common/`, `tofhir-common/`, `tofhir-rxnorm/`).
Read those first. The essentials:

1. **`tofhir` ≡ Ignifyr in code.** Packages (`io.tofhir.*`), modules (`tofhir-*`), config keys, and
   Docker tags all still use the legacy `tofhir` name. A full rename to `ignifyr` is a planned, separate
   task — don't rename names piecemeal as a side effect of other work.
2. **Verify with Maven.** `mvn test` for unit tests (fast, no Docker); `mvn -B verify` for the full build
   incl. integration tests — which **need Docker running** (MongoDB/Kafka/onFHIR via TestContainers).
   Report test results before claiming a change works.
3. **Commit format (SRDC semantic commits):** `<emoji> <type>(<scope>): <subject>` — `:sparkles:` feat,
   `:bug:` fix, `:memo:` docs, `:recycle:` refactor, `:construction_worker:` build, `:white_check_mark:`
   test, `:green_heart:` ci, `:art:` style, `:wrench:` chore, `:zap:` perf. Imperative subject; optional
   `Fixes #N`.
4. **Format before committing:** `mvn scalafmt:format`.

PRs target `main`; CI runs `mvn -B verify`.
