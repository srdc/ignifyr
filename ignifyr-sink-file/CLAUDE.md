# ignifyr-sink-file — file-system sink

Community sink module that contributes the file-system **sink writer** to the engine through the
`IgnifyrExtension` SPI, and owns the **sink-format sub-SPI** so the output formats are pluggable.
Package root `io.ignifyr.sink.file`. Depends only on `ignifyr-engine`; bundled into `ignifyr-cli`
(community). Apache-2.0-clean — subject to the `ban-enterprise-deps` maven-enforcer gate. The
file-system **source** lives in `ignifyr-connector-file`.

## The two-level SPI (the mental model)
The engine's `ExtensionRegistry` discovers this sink; the module then runs its **own** ServiceLoader
(`FileSinkFormatRegistry`) to discover sink formats keyed by content type. **The engine is entirely
ignorant of file formats** — only this module dispatches on sink `contentType`. So adding an output
format is a pure module move: the enterprise `ignifyr-format-delta` (Delta sink) plugs in via its
own `META-INF/services` file with zero changes here. The writer stays format-agnostic.

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
- `FileSinkSupport` centralizes the partition-by-resource-type layout **and** the HDFS raw-text
  write path, so the enterprise Delta writer reuses identical machinery and only supplies its
  terminal write. Its `singleColumnJson` flag distinguishes ndjson (raw single column) from
  parquet/delta (parsed, partition columns injected). Mapped results without a `resourceType`
  discriminator are skipped with a warning (never routed to a literal "null" directory).
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
