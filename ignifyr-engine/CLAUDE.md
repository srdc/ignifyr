# ignifyr-engine — core mapping/transformation engine

The Ignifyr engine: reads source data with Spark, applies FHIR mappings, and writes FHIR
resources. Usable as a library or as the standalone CLI/batch tool. Package root
`io.ignifyr.engine`.

## Entry points
- `Boot` ([src/main/scala/io/ignifyr/engine/Boot.scala](src/main/scala/io/ignifyr/engine/Boot.scala))
  — standalone entrypoint; routes args to `cli` (interactive, default), `run --job <path>` (batch),
  or any extension-contributed command token (e.g. the REDCap module's `extract-redcap-schemas`)
  via `ExtensionRegistry.cliCommands`. It is the Main-Class of the shaded jar.
- `IgnifyrEngine` — central orchestrator wiring config, repositories, the Spark session, and the mapping-job manager.
- `execution/MappingJobLauncher` — the single home of the batch/streaming/scheduled job dispatch;
  both the CLI (`CommandLineInterface.runJob`) and the server's `ExecutionService` launch through it.
- CLI: `cli/CommandLineInterface` + `cli/command/*` (`Run`, `Load`, `Reload`, `Stop`, `Help`,
  `ListRunningMappings`, `ListPlugins`, …). Modules contribute commands via the `CliCommandProvider`
  SPI; `list-plugins` prints the installed extensions and everything they contribute (a CI gate on
  the edition boundary).

## Layout (`src/main/scala/io/ignifyr/engine/`)
- `config/` — `IgnifyrConfig`, `IgnifyrEngineConfig` (read the `ignifyr` HOCON block), `FunctionLibrariesConfig`.
- `spi/` — the ServiceLoader extension SPI: `IgnifyrExtension` (one per module; contributes source
  connectors, sinks, terminology/identity services, CLI commands, source-failure descriptors, schema
  inferrers, streaming/scheduling capabilities, `sparkConfContributions`, `extraCapabilities`),
  `ExtensionRegistry`, the provider traits — `SourceConnector`, `SinkProvider`, `CliCommandProvider`,
  `SourceSchemaInferrer`, `SourceFailureDescriptor`, `StreamingExecutionProvider`, `SchedulerProvider`,
  `MappingTaskPipeline`, and `TerminologyServiceProvider`/`IdentityServiceProvider` (both in
  `IntegratedServiceProviders.scala`) — plus `ExtensionHints` (error-message module coordinates) and
  `core/CoreExtension`. **`CoreExtension` registers only the built-in CLI commands and the
  `LocalTerminologyService`: the engine ships no concrete I/O.**
- `data/read/` — `BaseDataSourceReader` (abstract base) + `SourceHandler`, which wraps the read and
  dispatches via `ExtensionRegistry.sourceConnectors`. The file reader (`FileDataSourceReader`) and the
  SQL/Kafka/FHIR-server readers all live in their `ignifyr-connector-*` modules.
- `data/write/` — `BaseSinkWriter` **and, declared in the same file, `object SinkWriterFactory`**, which
  is where the `ExtensionRegistry.sinkProviders` lookup happens; `SinkHandler` orchestrates the write but
  receives an already-constructed writer. Every concrete sink lives in its own
  `ignifyr-sink-*` module: the FHIR-repository writer in `ignifyr-sink-fhir`, the file-system
  writer and its pluggable output formats in `ignifyr-sink-file` (a `FileSinkFormat` sub-SPI);
  JSON-source and Delta-sink formats are the enterprise `ignifyr-format-*` modules.
- `mapping/` — `MappingTaskExecutor`, `FhirMappingService`, `job/FhirMappingJobManager` (batch +
  delegating streaming/scheduling seams), `schema/` (load/convert schemas), `context/MappingContextLoader`,
  `fhirPath/FhirPathMappingFunctions`, `service/LocalTerminologyService`. Cron scheduling itself lives
  in the enterprise `ignifyr-runtime-scheduling` module (`SchedulerProvider`).
- `execution/` — `MappingJobLauncher`, `RunningJobRegistry` (tracks running Spark jobs), `processing/`
  (`ErroneousRecordWriter`, `FileStreamInputArchiver`), `log/ExecutionLogger`.
- `model/` — domain models (`FhirMapping`, `FhirMappingJob`, `FhirMappingTask`, `*SinkSettings`,
  `MappingJobSourceSettings`, `BatchingStrategy`, …) and `model/exception/`. Models for ALL source
  types stay here (even extracted connectors) so any job JSON parses everywhere.
- `repository/` — `mapping/FhirMappingFolderRepository` (file-backed mapping repository) +
  `ICachedRepository`.
- `env/` — `EnvironmentVariable` + `EnvironmentVariableResolver` (env-var substitution in job settings).
- `Execution.scala` (top level) — the Akka `ActorSystem` the registry reads root config from, and which
  the service-provider modules use.
- `util/` — helpers (`FileUtils`, `SparkUtil`, `CsvUtil`, …).

## Adding a new data source reader (common task)
1. Add/extend a source-settings model under `model/` (alongside `MappingJobSourceSettings`).
2. Create (or extend) an `ignifyr-connector-<x>` module with a reader extending `BaseDataSourceReader`
   — mirror `ignifyr-connector-sql`.
3. Register it via the module's `IgnifyrExtension` (`sourceConnectors`) and its
   `META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension` entry; add the module to the reactor
   and to the distributions that ship it (`ignifyr-cli` for community, `ignifyr-server` for enterprise).
4. Add a lightweight registration spec **in the new module** (the engine's own suites can't reach it —
   the dependency runs the other way). Heavy fixture-based suites depend on the shared `ignifyr-testkit`
   (test scope) for `IgnifyrTestSpec` + `OnFhirTestContainer` + fixtures; if the suite writes results to
   a FHIR server it also needs `ignifyr-sink-fhir` at test scope, since the writer is no longer in the
   engine.

## Tests
- Unit: `mvn test -pl ignifyr-engine` → suites in `io.ignifyr.test` (`wildcardSuites`). All six are
  **self-contained** `AnyFlatSpec`s — `ExtensionRegistrySpec`, `ListPluginsTest`,
  `FileStreamInputArchiverTest`, `RunningJobRegistryTest`, `FhirMappingJobExecutionTest`,
  `FhirMappingUtilityTest`. They do **not** use `IgnifyrTestSpec`: that harness lives in
  `ignifyr-testkit`, and the engine must never depend on the testkit (reactor cycle). No Docker.
- The engine has **no integration suites of its own** — there is no `io.ignifyr.integrationtest` source
  folder here (the pom's `integration-test` execution simply finds nothing). The suites that exercise the
  engine end-to-end live in the modules that supply the I/O: `ignifyr-connector-file`
  (`FhirMappingJobManagerTest`), `ignifyr-connector-sql` (`SqlSourceTest`), `ignifyr-runtime-scheduling`
  (`SchedulingTest`) — all Docker-requiring, all run by `mvn -B verify`.
- One suite: `mvn test -pl ignifyr-engine -Dsuffixes='.*ListPluginsTest'`. `-DwildcardSuites`/`-Dsuites`
  are ignored — the pom sets `wildcardSuites` explicitly, which beats the command-line property.
- ⚠️ FHIRPath function libraries are validated at the `install` phase — a `scala-maven-plugin`
  launcher runs `ValidateFhirPathFunctionLibraries` over `io.ignifyr.engine.mapping`. Keep every new
  FHIRPath function annotated with `@FhirPathFunction`, or `install` fails.
