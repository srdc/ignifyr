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
  may be installed. A second one now fails at **engine startup**, because `ExtensionRegistry.init()`
  force-materializes `streaming`; it previously surfaced only at first job launch (or incidentally, via
  `IgnifyrEngine` consulting it for the archive timer).
- Two deliberate error strategies coexist: `StreamingSinkHandler` **catches per-micro-batch exceptions
  and only logs** (the stream survives bad chunks), while `StreamingJobExecutor`'s task-level `.recover`
  logs FAILURE and **rethrows**.
  - That log line is the *only* record a failed micro-batch leaves, so it must carry the `Throwable`
    itself — **never `e.getMessage`**. Passing a `String` selects scala-logging's
    `error(String, Any*)` overload, whose varargs fill `{}` placeholders; since the message is already
    fully interpolated there is no placeholder, and both the argument and the stack trace are silently
    discarded.
- Checkpoint dirs are per-job **and** per-mapping-task so distinct streams never mix Spark offsets.

## Tests
Tier-split, one execution each.

**Short** (`wildcardSuites=io.ignifyr.runtime.streaming`, no Docker) — `StreamingSinkHandlerTest`
(`AnyFlatSpec`): drives a **`MemoryStream`** through `StreamingSinkHandler.writeStream` to exercise the
catch-and-continue behaviour, pushing one chunk at a time and calling `processAllAvailable()` between
them. It asserts both halves of that contract: the writer's chunk counter reaching 2 proves it was handed
a *further* chunk after one threw, and `isActive`/`exception.isEmpty` prove the exception never escaped
the `foreachBatch`. Keep the first — without it the suite would still pass if the stream produced only
the failing chunk and then nothing at all, which is indistinguishable from swallow-and-continue from the
query's side.
- ⚠️ **Drive the batches; never wait on a clock.** This suite used a `rate` source plus
  `awaitTermination(5000)`, and the pass depended on that window: Spark's streaming startup dominates it
  and varies by machine (the second chunk landed ~0.5s *after* the window on one developer box — only
  `stop()` blocking on the in-flight batch saved the assertion — and needed 16–42s on a slower one).
  Shortening the window to 2s reproduces the failure on demand. `processAllAvailable()` removes the
  dependency instead of widening it. Note the old `awaitTermination` never rethrew anything either —
  `StreamingSinkHandler` swallows per-micro-batch exceptions, so the query never fails and that half of
  the contract went unasserted; the second swallowed error it used to log was an `InterruptedException`
  from the `stop()` that cut the batch short.
- ⚠️ **The mock job's source settings must carry `asStream = true`.** `FhirMappingJobExecution` derives
  `isStreamingJob` from them, and that flag is what makes `SinkHandler.logMappingJobResult` call
  `ExecutionLogger.logExecutionResultForStreamingMappingTask` (stateless) instead of
  `logExecutionResultForChunk`, whose per-execution cache is only ever seeded by a **batch** `STARTED`
  log. Leave it at the default and every chunk throws `NoSuchElementException: key not found: <executionId>`
  after the write — swallowed by the handler, so the suite still passes while failing for a reason it
  never meant to test.

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
