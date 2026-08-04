# ignifyr-runtime-streaming — streaming execution capability

Enterprise module providing Spark structured-streaming execution as an installable capability behind
the engine's `StreamingExecutionProvider` SPI. Package root `io.ignifyr.runtime.streaming`. Bundled
into `ignifyr-server`, **not** `ignifyr-cli`. Its only production dependency is `ignifyr-engine`
(Spark streaming comes transitively), so it carries no enterprise-only libs.

The community engine can build a streaming source dataset, but **starting and writing** the streaming
queries lives here. Without this module a job with `asStream=true` (or streaming source settings)
parses fine and fails at launch with `MissingCapabilityException` naming
`com.pontegra.ignifyr:ignifyr-runtime-streaming`. This is a carve-out of code that previously lived in
`FhirMappingJobManager.startMappingJobStream` — the "one-folder move between editions" rule in action.

## Layout (`src/main/scala/io/ignifyr/runtime/streaming/`)
- `StreamingRuntimeExtension` — the `IgnifyrExtension` impl (`id = "runtime-streaming"`); overrides
  `streamingProvider` to `Some(new StreamingJobExecutor)`, contributes nothing else. Named in the sole
  `META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension` file.
- `StreamingJobExecutor` — implements `StreamingExecutionProvider.startMappingJobStream`: builds the
  writer, runs each task through the engine's `MappingTaskPipeline.runMappingTask`, writes via
  `StreamingSinkHandler`, logs STARTED/FAILURE, and returns `Map[taskName, Future[StreamingQuery]]`.
- `StreamingSinkHandler` (object) — `writeStream` wraps the engine's batch `SinkHandler.writeMappingResult`
  inside a Spark `foreachBatch`, so each micro-batch is written exactly like a batch chunk, with an
  explicit per-job/per-mapping-task `checkpointLocation`.

## Notes
- The engine's `ExtensionRegistry.streaming` is a single-capability `Option` — **at most one** provider
  may be installed (more is a config error).
- Two deliberate error strategies coexist: `StreamingSinkHandler` **catches per-micro-batch exceptions
  and only logs** (the stream survives bad chunks), while `StreamingJobExecutor`'s task-level `.recover`
  logs FAILURE and **rethrows**.
- Checkpoint dirs are per-job **and** per-mapping-task so distinct streams never mix Spark offsets.

## Tests
Tier-split, one execution each.

**Short** (`wildcardSuites=io.ignifyr.runtime.streaming`, no Docker) — `StreamingSinkHandlerTest`
(`AnyFlatSpec`, mockito-scala): drives a `rate` stream through `StreamingSinkHandler.writeStream` to
exercise the catch-and-continue behaviour. Note it contains **no explicit expectation** — it starts the
query, `awaitTermination(5000)`, `stop()`, so the only failure signal is `awaitTermination` rethrowing if
the query died. It fails if a micro-batch exception escapes; it cannot assert that one was swallowed.

**Long** (`membersOnlySuites=io.ignifyr.integrationtest`, gated on `${skipITs}`, Docker) — two
end-to-end suites that exercise the real capability seam rather than the mechanics:
- `StreamingFolderWatchTest` — a CSV dropped into a watched directory *after* the query starts must flow
  through the testkit's `patient-mapping` and land as FHIR Patients in the onFHIR container. Uses
  `archiveMode = off` so it does not depend on the archiver timer (which only `IgnifyrEngine` starts).
- `KafkaStreamingRedcapTest` — a `KafkaContainer` stands in for the external `tofhir-redcap` service:
  REDCap-shaped JSON records are published to a topic and consumed through an ordinary `KafkaSource`.

Both mix in `IgnifyrTestSpec` + `OnFhirTestContainer`, so the module test-depends on `ignifyr-testkit`,
`ignifyr-connector-file`, `ignifyr-connector-kafka` and `ignifyr-sink-fhir` — the connectors and the sink
must be on the test classpath to be ServiceLoader-discovered, which is the same seam the production
edition boundary uses. Run them with `mvn -B verify -pl ignifyr-runtime-streaming -DskipITs=false`, and
clear `checkpoint/` + `logs/` first or the streaming suites trip `CONCURRENT_STREAM_LOG_UPDATE`.
