# ignifyr-testkit — shared test harness

The cross-reactor test harness: `IgnifyrTestSpec`, `OnFhirTestContainer`, `FhirMappingResultFixtures`,
and the classpath fixtures (mappings/schemas/terminology/data) that fixture-driven suites across
modules depend on. Community (`io.ignifyr`), **test-only — never bundled into a distribution**.

Why a module and not an engine test-jar: the harness must be consumable by the **plugin** modules, which
already depend on the engine, and an engine test-jar consumed cross-repo can't serve folder fixtures
reliably. It lives downstream of the engine (`ignifyr-testkit` → `ignifyr-engine`), so **`ignifyr-engine`
must never depend on it** — that would be a reactor cycle. (The engine's own six suites are
self-contained and use none of this; the cycle rule is what keeps it that way.) Consumers depend on it in
**test** scope. It opts into the community `ban-enterprise-deps` enforcer gate.

It is also load-bearing for the edition split: a **community** artifact whose fixtures **enterprise**
suites consume (`ignifyr-format-delta`'s `DeltaSinkFormatTest`). After the repo split the enterprise repo
can depend on the published community testkit; the reverse would be impossible.

## Layout
- `src/main/scala/io/ignifyr/` — the public surface: `IgnifyrTestSpec` (trait + companion),
  `OnFhirTestContainer` (object + mix-in trait), `FhirMappingResultFixtures` (object).
- `src/main/resources/` — fixtures mounted at fixed classpath roots: `/test-mappings` (+ context CSVs;
  nested `some-folder-*` exercise multi-folder discovery), `/test-schemas`, `/terminology-service`
  (code-system + concept-map CSVs), `/test-data` (`loop.json`).
- `src/test/scala/io/ignifyr/test/` — the module's **own** suites, relocated here from the engine:
  `FhirMappingFolderRepositoryTest`, `LocalTerminologyServiceTest`, `FhirPathMappingFunctionsTest`
  (all `AsyncFlatSpec` mixing in `IgnifyrTestSpec`; no Docker).

## Key seams
- `IgnifyrTestSpec` (trait, `io.ignifyr`) — what suites mix in. Eagerly builds a
  `FhirMappingFolderRepository`, a `MappingContextLoader`, a schema loader, `IgnifyrConfig.sparkSession`,
  and a `RunningJobRegistry`.
  ⚠️ **The fixture roots are not hardcoded** — it reads
  `IgnifyrConfig.engineConfig.mappingRepositoryFolderPath` and `.schemaRepositoryFolderPath`, so every
  consuming module must set `ignifyr.mappings.repository.folder-path = "/test-mappings"` and
  `ignifyr.mappings.schemas.repository.folder-path = "/test-schemas"` in its own
  `src/test/resources/application.conf` (connector-file, connector-sql, sink-file, format-json,
  format-delta and runtime-scheduling all do). Omitting it is the first failure a new consumer hits.
- **Jar-safe fixtures (the central trick):** `IgnifyrTestSpec.resolveResourceFolder` returns `file:`
  URLs directly, but for `jar:` URLs (published artifact) it materializes the folder subtree into a
  temp dir first, because the folder repositories call `new File(uri)`, which can't open a
  non-hierarchical `jar:` URL. **Use `testResourceFolderPath(path)` for any shared fixture folder — not
  `Paths.get(getClass.getResource(...).toURI)`.** (The rule bites only cross-module, which is why this
  module's own `LocalTerminologyServiceTest` gets away with the discouraged form; don't copy it.)
- `copyResourceFile(path)` — stages a single fixture file (e.g. a CSV under a context path) for suites
  that need a real file on disk.
- `OnFhirTestContainer` — lazy Testcontainers stack (`mongo:7.0` + `srdc/onfhir:r5` on a shared
  network, `withReuse(true)`); `getOnFhirClient` returns an `OnFhirNetworkClient`. For **consumer**
  integration suites; the testkit's own suites need no Docker.
- `FhirMappingResultFixtures.sampleFhirMappingResults(spark)` — a fixed 10-Patient/5-Condition dataset
  shared by sink-format writer suites in **both** editions (community `ignifyr-sink-file`'s
  `FileSystemWriterTest` + enterprise `ignifyr-format-delta`'s `DeltaSinkFormatTest`), which is why it
  lives in the community testkit. (Note: `ignifyr-connector-file` is source-only and no longer uses it.)

## Wiring gotchas
- The test-support libraries (scalatest, mockito-scala, h2, testcontainers + mongodb + junit-jupiter)
  are declared **compile scope** here, overriding the root pom's managed `test` scope — so a single
  `test`-scope dependency on `ignifyr-testkit` transitively supplies a consumer with the whole toolchain.
  Nine modules rely on this.
- Its own suites are pinned to `wildcardSuites=io.ignifyr.test` (short tier, no Docker) — the tier gate
  `test-flow/check-test-tiers.sh` forbids a bare `scalatest-maven-plugin` block anywhere, so the package
  convention is enforced here too. There is no integration-test execution in this module: the containers
  it *provides* (`OnFhirTestContainer`) are started by consumers' long-tier suites, not by its own.
- No `META-INF/services` and no `IgnifyrExtension`: this is a plain library, not a runtime plugin.
