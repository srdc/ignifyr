# ignifyr-common — Spark-free shared helpers

Four small files at the base of the dependency graph. Package root `io.ignifyr.common`. Community
(Apache-2.0); its pom opts into the shared `ban-enterprise-deps` enforcer gate. It holds **no** models
and **no** extension SPI — the SPI lives downstream in `ignifyr-engine` (`io.ignifyr.engine.spi`), and
the domain models live in `ignifyr-engine`'s `model/`.

Its whole value is being usable *without* Spark or the engine on the classpath: it is the module through
which `onfhir-common` and `onfhir-definition-commons` enter the build (the engine declares neither).
Honest caveat for anyone weighing a change: that property has no real consumer today — every module that
uses this code reaches it transitively through `ignifyr-engine`, so treat the split as historical rather
than load-bearing. Changes are still API changes to both editions; keep them backward-compatible.

**Note the artifactId: `ignifyr-common`, with no `_2.13` suffix** (only this module and
`ignifyr-server-common` are unsuffixed), even though it is Scala 2.13 code.

## Contents (`src/main/scala/io/ignifyr/common/`)
- `app/AppVersion` — *reads* `application.version` from a classpath `version.properties`, falling back
  to `"UNKNOWN"`. It does not ship that file: the filtered `version.properties` lives in
  **`ignifyr-engine`** and **`ignifyr-server`** resources (`<filtering>true</filtering>` in those poms).
  On a classpath with neither, `getVersion` returns `"UNKNOWN"`.
- `util/CustomMappingFunctions` — an onFHIR `AbstractFhirPathFunctionLibrary` exposing one `cst:`
  function, `createTimeSeriesData`, plus `CustomMappingFunctionsFactory` in the same file.
  ⚠️ No Scala code anywhere references the factory. It is wired by HOCON class name only —
  `ignifyr.functionLibraries.cst.className` — which is **active in `ignifyr-server`'s application.conf
  and commented out in `ignifyr-engine`'s**, so `cst:` functions are off by default in the community CLI.
- `util/SchemaUtil` — `convertToStructureDefinitionResource`: onFHIR `SchemaDefinition` →
  FHIR `StructureDefinition` as json4s JSON. Its main caller is enterprise server code.
- `util/ExceptionUtil` — flattens a `Throwable` cause chain into one newline-joined message.

## Tests
None. There is no `src/test` and no `scalatest-maven-plugin` execution, so `mvn test -pl ignifyr-common`
runs nothing. `SchemaUtil`/`ExceptionUtil` are exercised only indirectly (server schema endpoints,
engine mapping-error paths); `AppVersion` and `CustomMappingFunctions` have no coverage at all.
