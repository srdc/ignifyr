# ignifyr-server — REST API server

Akka-HTTP server exposing a REST API to manage Ignifyr projects, mappings, schemas, jobs,
terminology systems, and to trigger executions. Package root `io.ignifyr.server`. Depends on
`ignifyr-engine` and `ignifyr-server-common`.

## Entry & wiring
- `Boot` → `IgnifyrServer.start()` → `IgnifyrHttpServer` binds Akka-HTTP (default port **8085**, base path `/ignifyr`).
- `endpoint/IgnifyrServerEndpoint` assembles the full route tree (`ignifyrRoute`) from the per-resource endpoints.

## Layout (`src/main/scala/io/ignifyr/server/`)
- `endpoint/` — one class per resource: `ProjectEndpoint`, `MappingEndpoint`, `SchemaDefinitionEndpoint`,
  `JobEndpoint`, `MappingContextEndpoint`, `TerminologyServiceManagerEndpoint`, `CodeSystemEndpoint`,
  `ConceptMapEndpoint`, `ReloadEndpoint`, `MetadataEndpoint`, `FileSystemTreeStructureEndpoint`,
  plus the root `IgnifyrServerEndpoint`. Additional routes are contributed by installed modules via
  the `IgnifyrServerExtension` SPI in `ignifyr-server-common` (e.g. `ignifyr-redcap`'s `/redcap`
  proxy and `/projects/{id}/schemas/redcap` import).
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
`mvn test -pl ignifyr-server` → endpoint suites under `io.ignifyr.server.endpoint`. Several use a
MongoDB TestContainer, so **Docker must be running**.
