# ignifyr-sink-file — file-system sink

Community sink module that contributes the file-system **sink writer** to the engine through the
`IgnifyrExtension` SPI, and owns the **sink-format sub-SPI** so the output formats are pluggable.
Package root `io.ignifyr.sink.file`. Its only compile dependency is `ignifyr-engine`; **both**
distributions bundle it — `ignifyr-cli` and `ignifyr-server`. Apache-2.0-clean — subject to the
`ban-enterprise-deps` maven-enforcer gate. The file-system **source** lives in
`ignifyr-connector-file`.

It carries no third-party dependency of its own (Spark SQL and Hadoop `FileSystem` both come from the
engine), so it is not about dependency isolation. Note the **reverse** edge, which is the load-bearing
one: the enterprise `ignifyr-format-delta` declares a compile dependency on **this** module for the
`FileSinkFormat` sub-SPI and `FileSinkSupport` — a community artifact upstream of an enterprise one.
That is what keeps `delta-spark` out of the community jar while both sinks share identical machinery.

## The two-level SPI (the mental model)
The engine's `ExtensionRegistry` discovers this sink; the module then runs its **own** ServiceLoader
(`FileSinkFormatRegistry`) to discover sink formats keyed by content type. **The engine never resolves
a format handler** — only this module dispatches on sink `contentType`. So adding an output format is
essentially a module move: the enterprise `ignifyr-format-delta` (Delta sink) plugs in via its own
`META-INF/services` file. The writer stays format-agnostic.

Two qualifications: the engine *does* name the formats — `io.ignifyr.engine.model.SinkContentTypes`
hardcodes `ndjson`/`csv`/`parquet`/`delta`, including the enterprise-only one — and the **install-hint
table lives here**, in `FileSinkFormatHints.sinkFormatModules` (today just
`"delta" -> "com.pontegra.ignifyr:ignifyr-format-delta"`). So ServiceLoader registration alone is
zero-change, but a new pluggable format that wants a *named* install hint means editing this community
module; without an entry, an unknown content type gets only the generic "no handler registered for
content type 'x'" message.

## Layout (`src/main/scala/io/ignifyr/sink/file/`)
- top level — `FileSinkExtension` (the `IgnifyrExtension` impl), `FileSystemWriter` (sink dispatcher,
  a `BaseSinkWriter`).
- `format/` — the sub-SPI + plumbing: `FileSinkFormat` trait; `FileSinkFormatRegistry` (the
  module-local ServiceLoader registry); `FileSinkSupport` (shared write machinery);
  `FileSinkFormatHints` + `MissingFileSinkFormatException` (install-hint UX).
- `format/sink/` — community sink handlers: `NdjsonSinkFormat`, `CsvSinkFormat`, `ParquetSinkFormat`.
- `resources/META-INF/services/` — **two** files: one `io.ignifyr.engine.spi.IgnifyrExtension`
  (registers `FileSinkExtension`) and one for the sub-SPI (`…sink.file.format.FileSinkFormat` →
  ndjson/csv/parquet).

## Key seams
- `FileSystemWriter` is a thin dispatcher to `FileSinkFormatRegistry.sinkFormat(…)`.
- `FileSinkSupport` centralizes the partition-by-resource-type layout, so the enterprise Delta writer
  reuses identical machinery and only supplies its terminal write. Its `singleColumnJson` flag
  distinguishes ndjson (raw single column) from parquet/delta (parsed, partition columns injected).
  Mapped results without a `resourceType` discriminator are skipped with a warning (never routed to a
  literal "null" directory).
  - **There is deliberately no per-scheme branch.** Paths go to Spark's `DataFrameWriter` as given, so
    `hdfs://`, `s3a://` and plain local paths all work and all honour the requested content type plus
    `numOfPartitions`/`options`/`partitioningColumns`. Don't reintroduce one: the previous
    `path.startsWith("hdfs://")` special case bypassed the format handler entirely and wrote raw text,
    silently turning **parquet and delta** output into `.txt` on HDFS, and ignoring the sink settings for
    ndjson too (fixed 2026-07-25). CSV was never affected — it does not support `partitionByResourceType`
    and so never routes through this helper.
  - **Payloads never reach the driver.** Only the per-resource-type *counts* are collected; each type is
    then written from its own filtered `Dataset`. The narrowed two-column frame is `persist`ed for the
    duration (each type re-scans it) and unpersisted in a `finally` — it is a frame local to this helper,
    so a cache the caller holds on its own dataset is untouched. The earlier implementation
    `collect_list`-ed every mapped resource onto the driver; don't reinstate that pattern in a new sink.
- `FileSinkExtension.initialize()` force-materializes the sink-format registry at startup (mirrors
  `ExtensionRegistry.init()`) so a **duplicate** content-type fails fast at load — while a
  **missing** format stays lazy (a job naming it parses fine and fails only at first write with
  `MissingFileSinkFormatException`, whose message names the enterprise module to install).
  `extraCapabilities` surfaces the installed sink-format keys to the `list-plugins` command.

## Tests
**Unit only** (`wildcardSuites=io.ignifyr.sink.file`, no Docker): `FileSystemWriterTest` (ndjson/
parquet/csv writes, resource-type partitioning, discriminator-less-row skipping) and
`FileSinkExtensionSpec` (discovery, `extraCapabilities`, and the missing-format install-hint UX).
The shared test fixtures come from `ignifyr-testkit` (test scope). Note: `FileSystemWriterTest`
imports `sparkSession.implicits._` (not a delta encoder) precisely so these community writer tests
never pull `delta-spark` onto the classpath.
