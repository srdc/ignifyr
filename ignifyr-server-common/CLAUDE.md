# ignifyr-server-common — shared server infrastructure + the server extension SPI

Shared Akka-HTTP plumbing used by `ignifyr-server` (and any other server build) **and** the home of the
server-side extension SPI the edition split relies on. Package root `io.ignifyr.server.common`. Small and
foundational — changes ripple to every server endpoint and every server plugin, so keep them
backward-compatible.

## Shared plumbing
- `config/WebServerConfig` — host / port / base-uri (from the `webserver` HOCON block), plus SSL/keystore
  and server-location settings.
- `interceptor/ICORSHandler` — CORS directive trait mixed into endpoints.
- `interceptor/IErrorHandler` — maps exceptions → HTTP responses; also translates onFHIR
  `FhirDefinitionsError` into the module's own errors.
- `model/IgnifyrError` — abstract HTTP-error base plus the concrete taxonomy: `BadRequest` (400),
  `AlreadyExists` (409), `ResourceNotFound` (404), `InternalError` (500), `UnsupportedMediaType` (415),
  `MethodForbidden` (405), `RequestTimeout` (408). Pairs with `IErrorHandler`.
- `model/IgnifyrRestCall` — per-request context.

## The server extension SPI (`spi/`)
The server-side counterpart to the engine's `io.ignifyr.engine.spi.IgnifyrExtension`. Contributing a
server feature (routes, schema imports, a `/metadata` version) is done by adding an `IgnifyrServerExtension`
+ a `META-INF/services` entry in a plugin module, with **zero `ignifyr-server` code change** — mirroring the
engine's design rule (see `ignifyr-redcap`).
- `spi/IgnifyrServerExtension` — the SPI: `id`, `initialize(rootConfig)`, `rootRoutes`,
  `schemaImportRoutes(SchemaImportSink)` (mounted under `/projects/{id}/schemas` by `SchemaDefinitionEndpoint`),
  and `externalComponentVersion()` (surfaced via the server's `/metadata`).
- `spi/IgnifyrServerExtensions` — the ServiceLoader discovery object (`load(rootConfig)`): finds the
  extensions, initializes each, orders by id.
- `spi/SchemaImportSink` — a narrow persistence callback (`saveSchemas(projectId, schemas)`) handed to
  schema-import routes so an extension never touches the repository layer.

## Dependency & tests
Depends on `onfhir-resource-definitions` — `SchemaImportSink` works in onFHIR `SchemaDefinition`, and
`IErrorHandler` handles onFHIR `FhirDefinitionsError`. No tests live here; exercised through `ignifyr-server`.
