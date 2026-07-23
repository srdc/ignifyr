# ignifyr-runtime-scheduling — cron scheduling capability

Enterprise module providing cron-driven periodic batch execution as an installable capability behind
the engine's `SchedulerProvider` SPI, so the community engine carries **no** scheduling dependency.
Package root `io.ignifyr.runtime.scheduling`. Bundled into `ignifyr-server`, **not** `ignifyr-cli`.

It owns the cron4j scheduler, per-job last-synchronization bookkeeping (for incremental source syncs),
and the registry of scheduled executions — logic consolidated out of the engine's
`FhirMappingJobManager` + the old `MappingJobScheduler`. A job carrying `schedulingSettings` with no
provider installed parses fine and fails with `MissingCapabilityException`.

## Layout (`src/main/scala/io/ignifyr/runtime/scheduling/`)
- `SchedulingRuntimeExtension` — the `IgnifyrExtension` impl (`id = "runtime-scheduling"`); overrides
  `schedulerProvider` to `Some(new Cron4jSchedulerProvider)`. Named in the sole
  `META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension` file. Surfaced as `ExtensionRegistry.scheduler`
  (single-capability — at most one).
- `Cron4jSchedulerProvider` — the `SchedulerProvider` impl. Validates the cron expression, requires a
  non-empty `ignifyrDbFolderPath`, owns/starts the cron4j `Scheduler`, and on each fire runs an
  incremental batch over `(lastSyncTime, now)`, persisting the new sync time. The provider now owns the
  full start/register/deschedule dance — callers only schedule and deschedule.
- `ScheduledJobRegistry` — in-memory `jobId → executionId → (Scheduler, execution)` store (the
  scheduling counterpart of the engine's `RunningJobRegistry`). Bridges each cron fire into
  `RunningJobRegistry` (register on taskLaunching, `handleCompletedBatchJob` on success/failure) and
  logs SCHEDULED / DESCHEDULED.

## Notes
- **cron4j (`it.sauronsoftware.cron4j`) is enterprise-only** and banned from community modules by the
  root `ban-enterprise-deps` enforcer gate; this module and `ignifyr-server` are the only ones allowed
  to carry it.
- Incremental-sync state is a filesystem convention: one append-only file per job at
  `<ignifyrDbFolderPath>/scheduler/<jobId>.txt` whose last line is the previous sync timestamp;
  missing file → seed from `initialTime` (default epoch 0).
- Documented gotcha: `descheduleJobExecution` cannot actually stop an **in-flight** scheduled run
  (Spark distributes tasks across threads; job-group cancellation doesn't apply) — see the TODO in code.

## Tests
scalatest split. **Unit** (`wildcardSuites=io.ignifyr.runtime.scheduling`, no Docker):
`SchedulingRuntimeExtensionSpec` (ServiceLoader discovery + empty-registry state). **Integration**
(`membersOnlySuites=io.ignifyr.integrationtest`, Docker): `SchedulingTest` drives a full cron/SQL→FHIR
incremental sync against H2 + an onFHIR TestContainer (sleeps ~61s for one every-minute fire, then
deschedules). Test-scope deps: `ignifyr-testkit` (harness + fixtures) and `ignifyr-connector-sql` (so
the SqlSource reader is ServiceLoader-discovered on the test classpath).
