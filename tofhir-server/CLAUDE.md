# tofhir-server — REST API server

Akka-HTTP server exposing a REST API to manage Ignifyr projects, mappings, schemas, jobs,
terminology systems, and to trigger executions. Package root `io.tofhir.server`. Depends on
`tofhir-engine` and `tofhir-server-common`. (Naming: `tofhir` ≡ Ignifyr — see root [CLAUDE.md](../CLAUDE.md).)

## Entry & wiring
- `Boot` → `ToFhirServer.start()` → `ToFhirHttpServer` binds Akka-HTTP (default port **8085**, base path `/tofhir`).
- `endpoint/ToFhirServerEndpoint` assembles the full route tree (`toFHIRRoute`) from the per-resource endpoints.

## Layout (`src/main/scala/io/tofhir/server/`)
- `endpoint/` — one class per resource: `ProjectEndpoint`, `MappingEndpoint`, `SchemaDefinitionEndpoint`,
  `JobEndpoint`, `MappingContextEndpoint`, `TerminologyServiceManagerEndpoint`, `CodeSystemEndpoint`,
  `ConceptMapEndpoint`, `RedCapEndpoint`, `ReloadEndpoint`, `MetadataEndpoint`,
  `FileSystemTreeStructureEndpoint`, plus the root `ToFhirServerEndpoint`.
- `service/` — business logic per resource (`ProjectService`, `MappingService`, `JobService`,
  `ExecutionService`, `SchemaDefinitionService`, terminology services…).
- `repository/` — file-backed persistence; an interface + a `*FolderRepository` impl per resource
  (`IProjectRepository`/`ProjectFolderRepository`, `IJobRepository`/`JobFolderRepository`, mapping,
  mappingContext, schema, terminology). `FolderDBInitializer` + `FolderRepositoryManager` bootstrap the
  on-disk repo (project index `projects.json`).
- `model/` — request/response + domain models (`Project`, `ExecuteJobTask`, `InferTask`, …).
- `util/` — `ToFhirRejectionHandler`, `FileOperations`, `CsvUtil`, `DataFrameUtil`.

The layered pattern is **Endpoint (route/marshalling) → Service (logic) → Repository (persistence)**.
Cross-cutting CORS/error handling and `WebServerConfig` come from `tofhir-server-common`.

## Adding / changing an endpoint
1. Add or extend the `*Endpoint` (route + directives), delegating to a `*Service`.
2. Add the `*Service` logic; persist via a `*Repository` (interface `repository/<area>/I*.scala`,
   folder impl `*FolderRepository`).
3. Register the route in `ToFhirServerEndpoint`.
4. **Update [api.yaml](api.yaml)** (OpenAPI spec) to match the change.
5. Add a suite extending `BaseEndpointTest` (`AnyWordSpec` + `ScalatestRouteTest`); use `createProject()`
   for tests that need an existing project.

## Config & run
Reads HOCON sections `tofhir` (engine), `webserver` (`WebServerConfig` — host/port/base-uri),
`fhir` (definitions), and `tofhir-redcap`. Run:
`java -Dconfig.file=<tofhir.conf> -jar target/tofhir-server-standalone.jar`.

## Tests
`mvn test -pl tofhir-server` → endpoint suites under `io.tofhir.server.endpoint`. Several use a
MongoDB TestContainer, so **Docker must be running**.
