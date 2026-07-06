# ignifyr-rxnorm — RxNorm terminology client

Standalone module: a client for the public RxNorm REST API plus a FHIRPath function library so
mappings can call RxNorm lookups. Package root `io.ignifyr.rxnorm`.

- `RxNormApiClient` — HTTP client for RxNorm endpoints.
- `RxNormApiFunctionLibrary` — FHIRPath functions backed by the client (registered with onFHIR's
  FHIRPath engine; keep functions annotated with `@FhirPathFunction`).
- `PullRxNormNdcMedDetails` — utility/runner for bulk NDC → medication-detail pulls.

## Tests
`RxNormApiClientTest`, `RxNormApiFunctionLibraryTest` (in `src/test/scala`). ⚠️ These call the
**live RxNorm API** — failures are usually network/availability, not regressions. Run:
`mvn test -pl ignifyr-rxnorm`.
