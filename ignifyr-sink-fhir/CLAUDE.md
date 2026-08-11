# ignifyr-sink-fhir — FHIR-repository sink

Community sink module: writes mapped FHIR resources into a FHIR repository as batch bundles, and — from
the same extension — supplies the **FHIR-server-backed terminology and identity services**. Package root
`io.ignifyr.sink.fhir`. Compile dependency on `ignifyr-engine` + `onfhir-client`;
`ignifyr-testkit` at test scope. **Both** distributions bundle it (`ignifyr-cli` and `ignifyr-server`).
Apache-2.0-clean — opts into the `ban-enterprise-deps` maven-enforcer gate.

**Why a module — honestly, structural rather than dependency-driven.** `FhirRepositorySinkSettings` is an
engine model whose `createOnFhirClient` returns an `OnFhirNetworkClient`, so `onfhir-client` is an
`ignifyr-engine` dependency regardless; this module's own declaration of it keeps nothing out of the
community jar. What the extraction buys: the engine has **no privileged built-in sink**, so
`SinkProvider`/`ExtensionRegistry.sinkProviders` is the single dispatch path with no shortcut around it —
which is exactly what makes a future output target (`ignifyr-sink-omop`) a pure module add. It also makes
the flagship FHIR writer droppable: a file-only or OMOP-only deployment can simply omit it.

## The three-hooks-from-one-extension shape
`FhirSinkExtension` (`id = "sink-fhir"`, one `META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension`
entry) is the only extension in the repo registering three different hooks, **all keyed on the same
class, `FhirRepositorySinkSettings`**:
- `sinkProviders` → `FhirRepositoryWriter` (provider id `"fhir-repository"`).
- `terminologyServiceProviders` → onFHIR's `TerminologyServiceClient`.
- `identityServiceProviders` → `IdentityServiceClient`.

Consequence worth remembering: **this module is the sole provider of *any* identity service**, and of the
server-backed terminology service. A job that resolves hashed identities, or that points its terminology
settings at a FHIR server, needs this module installed **even if it writes somewhere else entirely**.
Both service providers take the actor system from `io.ignifyr.engine.Execution.actorSystem`.

It owns no sub-SPI (unlike `ignifyr-sink-file`) and overrides no `initialize`, `extraCapabilities`, or
`sparkConfContributions`.

## `FhirRepositoryWriter` — the non-obvious contracts
Read these before touching the write path; each exists because of a specific server's behaviour.
- **Batch size comes from engine config, not sink settings** —
  `IgnifyrConfig.engineConfig.fhirWriterBatchGroupSize` grouped over the partition.
- **Firely returns HTTP 400 for a batch where *any* entry fails** (onFHIR does not), which surfaces as a
  `FhirClientException`. That path is caught and re-derived into per-entry problems via
  `groupOutcomeIssuesByEntryIndex`, using the `OutcomeIssue` *expression* to locate the offending entry —
  if Firely omits the expression, the issue cannot be attributed and is logged as such.
- **Responses are matched to inputs positionally**, not by the `urn:uuid:` the request assigns. The
  UUID-matching approach worked with onFHIR but HAPI FHIR does not echo the UUID back. The request UUIDs
  are still generated (they are needed for intra-bundle references); just don't reintroduce a lookup by
  them.
- **409 Conflict is treated as transient and retried up to 4 attempts** (`checkResults` →
  `retryRequestsWithTransientError`); anything `hasNonTransientErrors` sees is failed immediately.
- Problems are attributed to the shared accumulator with the taxonomy `INVALID_RESOURCE` (the resource
  was rejected) vs `SERVICE_PROBLEM` (the server/transport failed) — the same split later sinks should
  mirror.
- `validate()` checks the configured FHIR repository URL is reachable.

## Tests
Two unit suites, both plain `AnyFlatSpec`s (neither mixes in `IgnifyrTestSpec`; the testkit dependency is
there mainly for scalatest). No Docker. `mvn test -pl ignifyr-sink-fhir`.
- `FhirSinkExtensionSpec` — ServiceLoader discovery and the three registrations.
- `FhirRepositoryWriterTest` — the entry-attribution step of the Firely path: since Firely answers a
  batch with HTTP 400 when *any* entry failed, the writer re-derives which input produced which problem
  from the `OutcomeIssue` expressions alone. An unattributable issue is **dropped** (logged, not blamed on
  entry 0) — that is the behaviour pinned here, because a wrong index blames the wrong record.
  `groupOutcomeIssuesByEntryIndex` is `private[fhir]` so this can be asserted without a live server.

The writer's real behaviour is exercised by the Docker integration suites in the modules that produce
data — `ignifyr-connector-file`'s `FhirMappingJobManagerTest`, `ignifyr-connector-sql`'s `SqlSourceTest`,
`ignifyr-runtime-scheduling`'s `SchedulingTest` — each of which test-depends on **this** module so the
sink provider is discoverable.
