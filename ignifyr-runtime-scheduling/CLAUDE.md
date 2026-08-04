# ignifyr-runtime-scheduling — cron scheduling capability

Enterprise module providing cron-driven periodic batch execution as an installable capability behind
the engine's `SchedulerProvider` SPI, so the community engine carries **no** scheduling dependency.
Package root `io.ignifyr.runtime.scheduling`. Bundled into `ignifyr-server`, **not** `ignifyr-cli`.

It owns the cron4j scheduler, per-job last-synchronization bookkeeping (for incremental source syncs),
and the registry of scheduled executions — logic physically moved out of the engine's
`FhirMappingJobManager` (`scheduleMappingJob`/`runnableMappingJob`/`getScheduledTimeRange`) and a
now-deleted `MappingJobScheduler` class (don't go looking for it — it exists only in git history). A job
carrying `schedulingSettings` with no provider installed parses fine and fails with
`MissingCapabilityException`.

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
  root `ban-enterprise-deps` enforcer gate. This module is its **sole declaration site** outside root
  `dependencyManagement` — `ignifyr-server` never declares it and only gets it transitively through this
  module. Note the gate is **opt-in, not a whitelist**: only the 8 poms declaring the
  `ban-enterprise-deps` execution are constrained, so any other enterprise module could carry cron4j and
  simply doesn't.
- Incremental-sync state is a filesystem convention: one append-only file per job at
  `<ignifyrDbFolderPath>/scheduler/<jobId>.txt` whose last line is the previous sync timestamp;
  missing file → seed from `initialTime` (default epoch 0).
- Documented gotcha: `descheduleJobExecution` cannot actually stop an **in-flight** scheduled run
  (Spark distributes tasks across threads; job-group cancellation doesn't apply) — see the TODO in code.

## Tests
Tier-split. **Short** (`wildcardSuites=io.ignifyr.runtime.scheduling`, no Docker):
`SchedulingRuntimeExtensionSpec` (ServiceLoader discovery + empty-registry state). **Long**
(`membersOnlySuites=io.ignifyr.integrationtest`, gated on `${skipITs}`, Docker): `SchedulingTest` drives a
full cron/SQL→FHIR incremental sync against H2 + an onFHIR TestContainer (sleeps ~61s for one
every-minute fire, then deschedules) — `mvn -B verify -pl ignifyr-runtime-scheduling -DskipITs=false`. **Three** test-scope Ignifyr deps, all needed for ServiceLoader discovery on the test
classpath: `ignifyr-testkit` (harness + fixtures), `ignifyr-connector-sql` (the `SqlSource` reader) and
`ignifyr-sink-fhir` (the test writes to the onFHIR container via `FhirRepositorySinkSettings`, and since
the sink split that writer is no longer in the engine).
