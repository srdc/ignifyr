# ignifyr-common — shared models & utilities

Cross-module shared code — the base of the dependency graph and the shared onFHIR-type wrapper layer.
Package root `io.ignifyr.common`. `ignifyr-engine` and `ignifyr-server` depend on it directly and every
plugin module inherits it transitively via the engine, so treat changes as API changes and keep them
backward-compatible. Community (Apache-2.0); its pom enforces the shared `ban-enterprise-deps` gate. It
holds no extension SPI — that lives in `ignifyr-engine` (`io.ignifyr.engine.spi`).

- `app/AppVersion` — single source of the build/app version.
- `util/CustomMappingFunctions` — custom functions made available inside mappings, plus
  `CustomMappingFunctionsFactory` (onFHIR `IFhirPathFunctionLibraryFactory`) in the same file — the
  factory the engine registers to expose them.
- `util/SchemaUtil` — schema (FHIR `StructureDefinition`) helpers.
- `util/ExceptionUtil` — shared exception formatting.

No standalone test suite; covered via the engine/server tests.
