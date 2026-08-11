# ignifyr-rxnorm — RxNorm terminology client

Standalone module: a client for the public RxNorm REST API plus a FHIRPath function library so
mappings can call RxNorm lookups. Package root `io.ignifyr.rxnorm`. It is **not** an `IgnifyrExtension`
SPI plugin and is **not** ServiceLoader-discovered — it plugs in through onFHIR's FHIRPath
function-library config (note the `ignifyr` parent block; the key is
`ignifyr.functionLibraries.rxn.className`):

```hocon
ignifyr {
  functionLibraries {
    rxn {
      className = "io.ignifyr.rxnorm.RxNormApiFunctionLibraryFactory"
      args = ["https://rxnav.nlm.nih.gov", 2]
    }
  }
}
```

That block is active in `ignifyr-server`'s `application.conf` and a commented example in
`ignifyr-engine`'s. Bundled into `ignifyr-server` only, **not** `ignifyr-cli`.

**Why a module:** it is an artifact boundary, not a code dependency. **Nothing in the repo compiles
against it** — no file outside the module imports `io.ignifyr.rxnorm`, and the module itself imports no
Ignifyr code at all (only onFHIR). Attaching or removing the `rxn:` functions is a config string plus a
jar on the classpath. Keeping it separate also keeps a blocking live-network HTTP client (global
singleton `ActorSystem`, `Await.result` per call) and `opencsv` out of the engine.

Two edition oddities worth knowing: its declared `ignifyr-common` dependency is **unused by main code**
(its only effect is dragging in `onfhir-definition-commons`, which a *test* needs), and although it
inherits the community `io.ignifyr` groupId it does **not** opt into the `ban-enterprise-deps` enforcer
gate the community modules all declare, while only the enterprise server bundles it. Its edition
placement is genuinely unsettled rather than principled.

## Contents (3 files in `src/main/scala/io/ignifyr/rxnorm/`)
- `RxNormApiClient` — HTTP client for RxNorm endpoints.
- `RxNormApiFunctionLibrary` — the `rxn:`-prefixed FHIRPath functions backed by the client
  (`findRxConceptIdsByNdc`, `getMedicationDetails`, `findIngredientsOfDrug`, `getATC`; keep them
  `@FhirPathFunction`-annotated). `RxNormApiFunctionLibraryFactory` — the onFHIR
  `IFhirPathFunctionLibraryFactory` that is the actual registration entry point (constructed with
  `rxNormApiRootUrl`, `timeoutInSec`) — is declared at the **bottom of this same file**, not in one of
  its own.
- `PullRxNormNdcMedDetails` — utility/runner for bulk NDC → medication-detail pulls.

It pins `com.opencsv:opencsv` to 5.5.1 inline, overriding the root-managed `opencsv.version`.

## Tests
**Short tier** (`wildcardSuites=io.ignifyr.rxnorm`, no Docker, **no network**):
`RxNormApiClientTest`, `RxNormApiFunctionLibraryTest`. `mvn test -pl ignifyr-rxnorm`.

Both run against `RxNormApiStub`, a local akka-http server bound on an ephemeral port that answers a
suite-declared map of canned bodies and **404s anything unlisted** — so an unexpected call fails loudly
instead of passing silently. The whole seam is the client's constructor: it takes its root url, so
pointing it at the stub needs no HTTP interception and no extra dependency (the stub uses the same
akka-http the client itself calls through). This is also the only way the not-found and non-200 branches
are reachable at all.

⚠️ Two things changed here on 2026-08-10 (`899faba7`) — older notes claiming otherwise are stale. The
suites used to sit in the **default package** and the module declared **no `scalatest-maven-plugin`**, so
they compiled, looked like coverage, and ran nowhere under Maven; they also called the live
`rxnav.nlm.nih.gov`, making a green build depend on a third party's uptime. `check-test-tiers.sh` now
carries an invariant (2b) that fails the build for any module with test sources and no plugin, so the
gap cannot reopen.
