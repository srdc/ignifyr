# ignifyr-testkit — shared test harness

The cross-reactor test harness: `IgnifyrTestSpec`, `OnFhirTestContainer`, `FhirMappingResultFixtures`,
and the classpath fixtures (mappings/schemas/terminology/data) that fixture-driven suites across
modules depend on. Community (`io.ignifyr`), **test-only — never bundled into a distribution**.

Why a module and not an engine test-jar: the engine's own suites need this harness, and an engine
test-jar consumed cross-repo can't serve folder fixtures reliably. It lives downstream of the engine
(`ignifyr-testkit` → `ignifyr-engine`), so **`ignifyr-engine` must never depend on it** — that would
be a reactor cycle. Consumers depend on it in **test** scope.

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
  `FhirMappingFolderRepository` over `/test-mappings`, a `MappingContextLoader`, a schema loader over
  `/test-schemas`, `IgnifyrConfig.sparkSession`, and a `RunningJobRegistry`.
- **Jar-safe fixtures (the central trick):** `IgnifyrTestSpec.resolveResourceFolder` returns `file:`
  URLs directly, but for `jar:` URLs (published artifact) it materializes the folder subtree into a
  temp dir first, because the folder repositories call `new File(uri)`, which can't open a
  non-hierarchical `jar:` URL. **Use `testResourceFolderPath(path)` for any shared fixture folder — not
  `Paths.get(getClass.getResource(...).toURI)`.**
- `OnFhirTestContainer` — lazy Testcontainers stack (`mongo:7.0` + `srdc/onfhir:r5` on a shared
  network, `withReuse(true)`); `getOnFhirClient` returns an `OnFhirNetworkClient`. For **consumer**
  integration suites; the testkit's own suites need no Docker.
- `FhirMappingResultFixtures.sampleFhirMappingResults(spark)` — a fixed 10-Patient/5-Condition dataset
  shared by sink-format writer suites in **both** editions (community connector-file + enterprise
  format-delta), which is why it lives in the community testkit.

## Wiring gotcha
The test-support libraries (scalatest, mockito-scala, h2, testcontainers + mongodb + junit-jupiter)
are declared **compile scope** here, overriding the root pom's managed `test` scope — so a single
`test`-scope dependency on `ignifyr-testkit` transitively supplies a consumer with the whole toolchain.
No `META-INF/services` and no `IgnifyrExtension`: this is a plain library, not a runtime plugin.
