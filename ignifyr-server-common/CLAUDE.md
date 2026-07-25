# ignifyr-server-common — shared server infrastructure + the server extension SPI

Shared Akka-HTTP plumbing used by `ignifyr-server` (and any other server build) **and** the home of the
server-side extension SPI the edition split relies on. Package root `io.ignifyr.server.common`. Five
source files; changes ripple to every server endpoint and every server plugin, so keep them
backward-compatible.

**Why it exists: to break a cycle.** A server-side plugin can never depend on `ignifyr-server` — that is
the distribution that shades it — yet both halves must compile against the same seam. So the seam lives
here, in a library that declares **no Ignifyr dependency at all** (not the engine, not
`ignifyr-common`): a server plugin can implement `IgnifyrServerExtension` without pulling in the engine
or Spark. Enterprise-side; it correctly does not opt into the `ban-enterprise-deps` enforcer gate, and it
is a plain jar (no shade).

**Note the artifactId: `ignifyr-server-common`, with no `_2.13` suffix** (only this module and
`ignifyr-common` are unsuffixed).

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
  and `externalComponentVersion()`. ⚠️ The last one is **not** generically surfaced: the server's
  `MetadataService` looks up `id == "redcap"` and reports only that extension's version, so
  implementing the hook in a different module has no effect today.
- `spi/IgnifyrServerExtensions` — the ServiceLoader discovery object (`load(rootConfig)`): finds the
  extensions, **sorts by id, then initializes each** (in that order).
- `spi/SchemaImportSink` — a narrow persistence callback (`saveSchemas(projectId, schemas)`) handed to
  schema-import routes so an extension never touches the repository layer.

## Dependency & tests
Declares `com.typesafe:config`, akka-http/-core, akka-stream-typed (provided), scala-logging, and
`onfhir-resource-definitions` — from which `IErrorHandler` takes `FhirDefinitionsError`. Note
`SchemaImportSink`'s `SchemaDefinition` actually comes from **`onfhir-definition-commons`**, reached
transitively. That single onFHIR dependency is not cheap: it drags in the whole onFHIR server stack
(definition-commons, client, config, r4, server-r4, server-r5) for two model types.

No tests live here; exercised through `ignifyr-server`.
