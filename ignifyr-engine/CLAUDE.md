# ignifyr-engine — core mapping/transformation engine

The Ignifyr engine: reads source data with Spark, applies FHIR mappings, and writes FHIR
resources. Usable as a library or as the standalone CLI/batch tool. Package root
`io.ignifyr.engine`.

## Entry points
- `Boot` ([src/main/scala/io/ignifyr/engine/Boot.scala](src/main/scala/io/ignifyr/engine/Boot.scala))
  — standalone entrypoint; routes args to `cli` (interactive, default), `run --job <path>` (batch),
  or `extract-redcap-schemas`. It is the Main-Class of the shaded jar.
- `IgnifyrEngine` — central orchestrator wiring config, repositories, the Spark session, and the mapping-job manager.
- CLI: `cli/CommandLineInterface` + `cli/command/*` (`Run`, `Load`, `Reload`, `Stop`, `Help`,
  `ListRunningMappings`, `ExtractRedCapSchemas`, …).

## Layout (`src/main/scala/io/ignifyr/engine/`)
- `config/` — `IgnifyrConfig`, `IgnifyrEngineConfig` (read the `ignifyr` HOCON block), `FunctionLibrariesConfig`.
- `data/read/` — source readers. `BaseDataSourceReader` is the abstract base; concrete readers are
  `FileDataSourceReader`, `SqlSourceReader`, `KafkaSourceReader`, `FhirServerDataSourceReader`.
  `DataSourceReaderFactory` picks one from the source settings; `SourceHandler` wraps the read.
- `data/write/` — `BaseFhirWriter` with `FhirRepositoryWriter` (to a FHIR server) and `FileSystemWriter`;
  `SinkHandler` orchestrates writes.
- `mapping/` — `MappingTaskExecutor`, `FhirMappingService`, `job/FhirMappingJobManager` +
  `MappingJobScheduler` (cron4j), `schema/` (load/convert schemas), `context/MappingContextLoader`,
  `fhirPath/FhirPathMappingFunctions`, `service/LocalTerminologyService`.
- `execution/` — `RunningJobRegistry` (tracks running Spark jobs), `processing/`
  (`ErroneousRecordWriter`, `FileStreamInputArchiver`), `log/ExecutionLogger`.
- `model/` — domain models (`FhirMapping`, `FhirMappingJob`, `FhirMappingTask`, `*SinkSettings`,
  `MappingJobSourceSettings`, `BatchingStrategy`, …) and `model/exception/`.
- `repository/mapping/` — `FhirMappingFolderRepository` (file-backed mapping repository).
- `util/` — helpers (`FileUtils`, `SparkUtil`, `CsvUtil`, REDCap + mapping-generator utilities).

## Adding a new data source reader (common task)
1. Add/extend a source-settings model under `model/` (alongside `MappingJobSourceSettings`).
2. Implement a reader extending `BaseDataSourceReader` in `data/read/` — mirror `FileDataSourceReader`
   or `SqlSourceReader`.
3. Wire it into `DataSourceReaderFactory`.
4. Add a suite under `src/test/scala/io/ignifyr/test/…` extending `IgnifyrTestSpec`; put sample inputs
   in `src/test/resources`.

## Tests
- Unit: `mvn test -pl ignifyr-engine` → suites in `io.ignifyr.test`, extending `IgnifyrTestSpec`
  (gives a `SparkSession`, the mapping/schema repositories, and a `RunningJobRegistry`).
- Integration: `io.ignifyr.integrationtest` (e.g. `KafkaSourceIntegrationTest`) runs in the
  `integration-test` phase via `mvn -B verify` and **requires Docker** (Kafka/MongoDB/onFHIR containers).
- ⚠️ FHIRPath function libraries are validated at the `install` phase — a `scala-maven-plugin`
  launcher runs `ValidateFhirPathFunctionLibraries` over `io.ignifyr.engine.mapping`. Keep every new
  FHIRPath function annotated with `@FhirPathFunction`, or `install` fails.
