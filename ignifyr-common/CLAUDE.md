# ignifyr-common — shared models & utilities

Cross-module shared code. Package root `io.ignifyr.common`. Depended on by every other module, so
treat changes as API changes and keep them backward-compatible.

- `app/AppVersion` — single source of the build/app version.
- `util/CustomMappingFunctions` — custom functions made available inside mappings.
- `util/SchemaUtil` — schema (FHIR `StructureDefinition`) helpers.
- `util/ExceptionUtil` — shared exception formatting.

No standalone test suite; covered via the engine/server tests.
