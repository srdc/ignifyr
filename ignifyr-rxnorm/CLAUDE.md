# ignifyr-rxnorm — RxNorm terminology client

Standalone module: a client for the public RxNorm REST API plus a FHIRPath function library so
mappings can call RxNorm lookups. Package root `io.ignifyr.rxnorm`. It is **not** an `IgnifyrExtension`
SPI plugin and is **not** ServiceLoader-discovered — it plugs in through onFHIR's FHIRPath
function-library config: `functionLibraries { rxn { className = "io.ignifyr.rxnorm.RxNormApiFunctionLibraryFactory", args = ["https://rxnav.nlm.nih.gov", 2] } }` (active in `ignifyr-server`'s `application.conf`, a commented example in `ignifyr-engine`'s). Bundled into `ignifyr-server` (enterprise) only, **not** `ignifyr-cli`; depends on `ignifyr-common` + onFHIR + opencsv.

- `RxNormApiClient` — HTTP client for RxNorm endpoints.
- `RxNormApiFunctionLibrary` — the `rxn:`-prefixed FHIRPath functions backed by the client
  (`findRxConceptIdsByNdc`, `getMedicationDetails`, `findIngredientsOfDrug`, `getATC`; keep them
  `@FhirPathFunction`-annotated).
- `RxNormApiFunctionLibraryFactory` — the onFHIR `IFhirPathFunctionLibraryFactory` that is the actual
  registration entry point (constructed with `rxNormApiRootUrl`, `timeoutInSec`); this is the class
  named in config, not the library directly.
- `PullRxNormNdcMedDetails` — utility/runner for bulk NDC → medication-detail pulls.

## Tests
`RxNormApiClientTest`, `RxNormApiFunctionLibraryTest` (in `src/test/scala`). ⚠️ These call the
**live RxNorm API** — failures are usually network/availability, not regressions. Run:
`mvn test -pl ignifyr-rxnorm`.
