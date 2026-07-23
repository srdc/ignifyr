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
  connectors, sinks, CLI commands, schema inferrers, streaming/scheduling capabilities, extra
  `sparkConfContributions`, …), `ExtensionRegistry`, the provider traits (`SourceConnector`,
  `SinkProvider`, `CliCommandProvider`, `SourceSchemaInferrer`, `SourceFailureDescriptor`,
  `StreamingExecutionProvider`, `SchedulerProvider`), `ExtensionHints` (error-message module
  coordinates), and `core/CoreExtension` (the engine's own registrations).
- `data/read/` — `BaseDataSourceReader` (abstract base) + `SourceHandler`, which wraps the read and
  dispatches via `ExtensionRegistry.sourceConnectors`. The file reader (`FileDataSourceReader`) and the
  SQL/Kafka/FHIR-server readers all live in their `ignifyr-connector-*` modules.
- `data/write/` — `BaseFhirWriter` + `FhirRepositoryWriter` (the community FHIR-server sink) and
  `SinkHandler` (orchestrates writes, dispatches via `ExtensionRegistry.sinkProviders`). The
  file-system writer (`FileSystemWriter`) and its pluggable output formats live in
  `ignifyr-connector-file` (a `FileSourceFormat`/`FileSinkFormat` sub-SPI); JSON-source and Delta-sink
  formats are the enterprise `ignifyr-format-*` modules.
- `mapping/` — `MappingTaskExecutor`, `FhirMappingService`, `job/FhirMappingJobManager` (batch +
  delegating streaming/scheduling seams), `schema/` (load/convert schemas), `context/MappingContextLoader`,
  `fhirPath/FhirPathMappingFunctions`, `service/LocalTerminologyService`. Cron scheduling itself lives
  in the enterprise `ignifyr-runtime-scheduling` module (`SchedulerProvider`).
- `execution/` — `MappingJobLauncher`, `RunningJobRegistry` (tracks running Spark jobs), `processing/`
  (`ErroneousRecordWriter`, `FileStreamInputArchiver`), `log/ExecutionLogger`.
- `model/` — domain models (`FhirMapping`, `FhirMappingJob`, `FhirMappingTask`, `*SinkSettings`,
  `MappingJobSourceSettings`, `BatchingStrategy`, …) and `model/exception/`. Models for ALL source
  types stay here (even extracted connectors) so any job JSON parses everywhere.
- `repository/mapping/` — `FhirMappingFolderRepository` (file-backed mapping repository).
- `util/` — helpers (`FileUtils`, `SparkUtil`, `CsvUtil`, …).

## Adding a new data source reader (common task)
1. Add/extend a source-settings model under `model/` (alongside `MappingJobSourceSettings`).
2. Create (or extend) an `ignifyr-connector-<x>` module with a reader extending `BaseDataSourceReader`
   — mirror `ignifyr-connector-sql`.
3. Register it via the module's `IgnifyrExtension` (`sourceConnectors`) and its
   `META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension` entry; add the module to the reactor
   and to the distributions that ship it (`ignifyr-cli` for community, `ignifyr-server` for enterprise).
4. Add a lightweight registration spec in the module; heavy fixture-based suites depend on the
   shared `ignifyr-testkit` (test scope) for `IgnifyrTestSpec` + `OnFhirTestContainer` + fixtures.

## Tests
- Unit: `mvn test -pl ignifyr-engine` → suites in `io.ignifyr.test`, extending `IgnifyrTestSpec`
  (gives a `SparkSession`, the mapping/schema repositories, and a `RunningJobRegistry`).
- Integration: `io.ignifyr.integrationtest` (e.g. `KafkaSourceIntegrationTest`) runs in the
  `integration-test` phase via `mvn -B verify` and **requires Docker** (Kafka/MongoDB/onFHIR containers).
- ⚠️ FHIRPath function libraries are validated at the `install` phase — a `scala-maven-plugin`
  launcher runs `ValidateFhirPathFunctionLibraries` over `io.ignifyr.engine.mapping`. Keep every new
  FHIRPath function annotated with `@FhirPathFunction`, or `install` fails.
