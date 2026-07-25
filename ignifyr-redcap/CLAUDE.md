# ignifyr-redcap — REDCap integration

Enterprise module for REDCap support. Two things: (1) an engine CLI command that converts a REDCap
data-dictionary CSV into FHIR `StructureDefinition` schemas, and (2) server REST routes that proxy the
sibling **tofhir-redcap** service plus a per-project data-dictionary schema-import route. Package root
`io.ignifyr.redcap` (flat). Bundled into `ignifyr-server`, **not** `ignifyr-cli`.

**Why a module:** dependency *direction*, not dependency isolation (its pom declares no third-party
compile dependency at all). It is the only plugin in the reactor depending on **both**
`ignifyr-engine` and `ignifyr-server-common`; living in the engine would force the engine to depend on
server code and invert the layering. It is also the only consumer of the `IgnifyrServerExtension` seam —
the module that justifies that SPI existing. Being enterprise, it does not opt into
`ban-enterprise-deps`.

## Defining trait: it registers against BOTH SPIs
Two `META-INF/services` files, one per SPI — this is the module's whole point (REDCap plugs into both
the engine and the server with zero core changes):
- `io.ignifyr.engine.spi.IgnifyrExtension` → `RedCapExtension` — the engine side: contributes the
  `extract-redcap-schemas` CLI command (formerly registered by the engine's `CoreExtension`).
- `io.ignifyr.server.common.spi.IgnifyrServerExtension` → `RedCapServerExtension` — the server side:
  contributes routes and the `/metadata` version. Both use `id = "redcap"`.

## Layout (`src/main/scala/io/ignifyr/redcap/`)
- `RedCapExtension` — engine `IgnifyrExtension`; its inline `CliCommandProvider.argsFromOptions` maps
  Boot one-shot flags (`data-dictionary`, `definition-root-url`, `encoding`) to positional args.
- `RedCapServerExtension` — server `IgnifyrServerExtension`; `initialize` best-effort parses the
  optional `ignifyr-redcap` HOCON block; `rootRoutes` contributes the `/redcap` proxy **only when that
  block is present**; `schemaImportRoutes` **always** contributes the import route;
  `externalComponentVersion()` GETs `<endpoint>/metadata` and returns the **entire unparsed response
  body** as the version string, with no timeout of its own — the 1-second bound and the
  swallow-on-failure live in the server's `MetadataService`, which files the result under
  `Metadata.ignifyrRedcapVersion`.
- `ExtractRedCapSchemas` — the CLI `Command`: reads the dictionary CSV, calls `RedCapUtil`, writes each
  `<Type>.StructureDefinition.json` into the schema repository folder (warns + overwrites on collision).
- `RedCapUtil` — the extraction object: one `SchemaDefinition` per REDCap Form; `getDataType` /
  `getCardinality` map REDCap field & text-validation types to FHIR; injects a record-id field if
  absent. `RedCapDataDictionaryColumns` / `RedCapDataTypes` / `RedCapTextValidationTypes` hold the
  column/value constants. Only `extractSchemasAsSchemaDefinitions` is pure — `extractSchemas` also reads
  `IgnifyrConfig.engineConfig.schemaRepositoryFhirVersion`.
  ⚠️ `recordIdField` defaults to `""`, and the CLI path takes that default — so CLI-extracted schemas get
  a record-identifier field with an empty `id` and a `"<Schema>."` path. Only the server import route
  passes a real value (a query parameter).
- `RedCapEndpoint` + `RedCapService` + `RedCapServiceConfig` — the `/redcap` proxy (projects,
  delete-with-reload, notification) to tofhir-redcap; `RedCapSchemaImportEndpoint` — the multipart
  `/projects/{id}/schemas/redcap` import (persists via the server-provided `SchemaImportSink`);
  `RedCapProjectConfig` — the proxied project payload.

## Notes
- The proxy targets the sibling **tofhir-redcap** service — a **sanctioned legacy `tofhir` survivor**.
  The default endpoint `http://localhost:8095/tofhir-redcap` and the `ignifyr-redcap` HOCON key keep
  that value on purpose; do **not** rename them to `ignifyr`.
- BOM gotcha: REDCap exports the dictionary as UTF-8-with-BOM, so `RedCapUtil` reads the variable-name
  column, then falls back to a BOM-prefixed column name before failing.
- The Kafka coupling (deleting a project's data from topics) lives in the external tofhir-redcap
  service — there is **no** Kafka dependency in this module.

## Tests
One unit suite, `RedCapExtensionSpec` (`AnyFlatSpec`; no Docker, no testkit): dual-SPI discovery, the
CLI flag→positional mapping, config-gated `rootRoutes`, and pure `RedCapUtil` extraction. Endpoint-level
import behaviour is covered by the server's `SchemaEndpointTest`. `mvn test -pl ignifyr-redcap`.
