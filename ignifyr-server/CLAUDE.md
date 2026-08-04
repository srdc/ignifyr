# ignifyr-server — REST API server

Akka-HTTP server exposing a REST API to manage Ignifyr projects, mappings, schemas, jobs,
terminology systems, and to trigger executions. Package root `io.ignifyr.server`.

**It is simultaneously the REST API and the enterprise standalone distribution** — the only module in
the reactor that is both (`ignifyr-cli`, its community counterpart, has no `src/` at all). Its
maven-shade assembly produces `target/ignifyr-server-standalone.jar` (Main-Class
`io.ignifyr.server.Boot`). Its POM declares **17** compile-scope `io.ignifyr` dependencies — engine,
common, server-common, rxnorm, `connector-sql`/`-file`/`-fhir-server`/`-kafka`,
`sink-fhir`/`-file`/`-omop`, `format-json`/`-delta`, `runtime-streaming`/`-scheduling`, redcap,
observability — plus `ignifyr-testkit` at test scope. **That dependency list is the definition of the
Enterprise edition**, so moving a feature between editions is one line here and one in
`ignifyr-cli/pom.xml`. It deliberately does *not* opt into the `ban-enterprise-deps` enforcer gate: it
legitimately pulls Kafka, Delta, cron4j and Logstash/Fluentd transitively.

Since the engine ships no concrete I/O, the sink modules are as load-bearing as the connectors — drop
`ignifyr-sink-fhir` and the server can no longer write to a FHIR repository. Everything is discovered at
runtime through the `IgnifyrExtension` / `IgnifyrServerExtension` ServiceLoader SPIs; no concrete plugin
type is imported anywhere in `src/main/scala`. Two string-level couplings are the exceptions to that
rule: `MetadataService` hardcodes the extension id `"redcap"`, and `src/main/resources/logback.xml`
hardcodes `io.ignifyr.observability.logback.MapMarkerToLogstashMarkerEncoder`.

## Entry & wiring
- `Boot` → `IgnifyrServer.start()` → `IgnifyrHttpServer` binds Akka-HTTP (default port **8085**, base path `/ignifyr`).
- `IgnifyrServer.start()` also runs `IgnifyrServerExtensions.load(rootConfig)` and threads the discovered
  `Seq[IgnifyrServerExtension]` into `IgnifyrServerEndpoint`, `MetadataEndpoint`, and (via `ProjectEndpoint`)
  `SchemaDefinitionEndpoint`.
- `endpoint/IgnifyrServerEndpoint` assembles the full route tree (`ignifyrRoute`) from the per-resource
  endpoints, concatenating each extension's `rootRoutes` (`extensionRootRoutes`).

## Layout (`src/main/scala/io/ignifyr/server/`)
- `endpoint/` — one class per resource: `ProjectEndpoint`, `MappingEndpoint`, `SchemaDefinitionEndpoint`,
  `JobEndpoint`, `MappingContextEndpoint`, `TerminologyServiceManagerEndpoint`, `CodeSystemEndpoint`,
  `ConceptMapEndpoint`, `ReloadEndpoint`, `MetadataEndpoint`, `FileSystemTreeStructureEndpoint`,
  plus the root `IgnifyrServerEndpoint`. Additional routes are contributed by installed modules via
  the `IgnifyrServerExtension` SPI in `ignifyr-server-common` (e.g. `ignifyr-redcap`'s `/redcap`
  proxy and `/projects/{id}/schemas/redcap` import). Two seams route those contributions:
  `SchemaDefinitionEndpoint` mounts each extension's `schemaImportRoutes(SchemaImportSink)` under
  `/projects/{id}/schemas` (the sink persists schemas without the module touching the repository
  layer), and `MetadataEndpoint` surfaces an extension's `externalComponentVersion()` in `/metadata`.
  ⚠️ That second seam is **not** generic today: `MetadataService` looks up `id == "redcap"` and
  publishes only that one result, as `Metadata.ignifyrRedcapVersion`. No other extension's version is
  ever queried.
- `service/` — business logic per resource (`ProjectService`, `MappingService`, `JobService`,
  `ExecutionService`, `SchemaDefinitionService`, terminology services…).
- `repository/` — file-backed persistence; an interface + a `*FolderRepository` impl per resource
  (`IProjectRepository`/`ProjectFolderRepository`, `IJobRepository`/`JobFolderRepository`, mapping,
  mappingContext, schema, terminology). `FolderDBInitializer` + `FolderRepositoryManager` bootstrap the
  on-disk repo (project index `projects.json`).
- `model/` — request/response + domain models (`Project`, `ExecuteJobTask`, `InferTask`, …).
- `util/` — `IgnifyrRejectionHandler`, `FileOperations`, `CsvUtil`, `DataFrameUtil`.

The layered pattern is **Endpoint (route/marshalling) → Service (logic) → Repository (persistence)**.
Cross-cutting CORS/error handling and `WebServerConfig` come from `ignifyr-server-common`.

## Adding / changing an endpoint
1. Add or extend the `*Endpoint` (route + directives), delegating to a `*Service`.
2. Add the `*Service` logic; persist via a `*Repository` (interface `repository/<area>/I*.scala`,
   folder impl `*FolderRepository`).
3. Register the route in `IgnifyrServerEndpoint` — or, for an optional/edition-specific feature,
   contribute it from its own module via `IgnifyrServerExtension` (see `ignifyr-redcap`).
4. **Update [api.yaml](api.yaml)** (OpenAPI spec) to match the change.
5. Add a suite extending `BaseEndpointTest` (`AnyWordSpec` + `ScalatestRouteTest`); use `createProject()`
   for tests that need an existing project.

## Config & run
Reads HOCON sections `ignifyr` (engine), `webserver` (`WebServerConfig` — host/port/base-uri),
`fhir` (definitions), and `ignifyr-redcap`. Run:
`java -Dconfig.file=<ignifyr.conf> -jar target/ignifyr-server-standalone.jar`.

## Tests
`mvn test -pl ignifyr-server` → suites under `io.ignifyr.server` (`BaseEndpointTest`),
`io.ignifyr.server.endpoint`, and `io.ignifyr.server.service` (`ExecutionServiceTest`).

The suites are **tier-split** like every other module (two `scalatest-maven-plugin` executions):
- **short** — `wildcardSuites=io.ignifyr.server`: the nine endpoint suites plus `ExecutionServiceTest`
  and `BaseEndpointTest`. Docker-free, runs in `mvn test`.
- **long** — `membersOnlySuites=io.ignifyr.integrationtest`, gated on `${skipITs}`: the three suites that
  mix in the testkit's `OnFhirTestContainer` (`FhirDefinitionsEndpointTest`, `SchemaEndpointTest`,
  `MappingExecutionEndpointTest`) and so start an `srdc/onfhir:r5` container backed by `mongo:7.0`. They
  live in `io.ignifyr.integrationtest`, not under `io.ignifyr.server.endpoint`, precisely because the
  package is what selects the tier. Run them with `mvn -B verify -DskipITs=false`.

`BaseEndpointTest` itself starts no container — it boots the route via `ScalatestRouteTest`. Historically
this module ran everything (containers included) in the `test` phase; `test-flow/check-test-tiers.sh` now
fails the build if a container-backed suite reappears in a short-tier package.
