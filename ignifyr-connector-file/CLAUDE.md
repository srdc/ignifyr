# ignifyr-connector-file — file-system source + sink connector

Community connector that contributes the file-system **source reader** and **sink writer** to the
engine through the `IgnifyrExtension` SPI, and owns a connector-local **file-format sub-SPI** so the
formats themselves are pluggable. Package root `io.ignifyr.connector.file`. Depends only on
`ignifyr-engine`; bundled into `ignifyr-cli` (community). Apache-2.0-clean — subject to the
`ban-enterprise-deps` maven-enforcer gate.

## The two-level SPI (the mental model)
The engine's `ExtensionRegistry` discovers this connector; the connector then runs its **own**
ServiceLoader (`FileFormatRegistry`) to discover file formats keyed by content type. **The engine is
entirely ignorant of file formats** — only this connector dispatches on `contentType`. So adding a
format is a pure module move: the enterprise `ignifyr-format-json` (JSON/NDJSON source) and
`ignifyr-format-delta` (Delta sink) plug in via their own `META-INF/services` files with zero changes
here. The reader and writer stay format-agnostic.

## Layout (`src/main/scala/io/ignifyr/connector/file/`)
- top level — `FileConnectorExtension` (the `IgnifyrExtension` impl), `FileDataSourceReader` (source),
  `FileSystemWriter` (sink dispatcher).
- `format/` — the sub-SPI + plumbing: `FileSourceFormat` (+ `FileSourceReadContext`), `FileSinkFormat`
  traits; `FileFormatRegistry` (the connector-local ServiceLoader registry); `FileSinkSupport` (shared
  write machinery); `FileFormatHints` + `MissingFileFormatException` (install-hint UX).
- `format/source/` — community source handlers: `CsvSourceFormat` (csv **and** tsv, plus zip via
  `SparkUtil.readZip`), `ParquetSourceFormat`.
- `format/sink/` — community sink handlers: `NdjsonSinkFormat`, `CsvSinkFormat`, `ParquetSinkFormat`.
- `resources/META-INF/services/` — **three** files: one `io.ignifyr.engine.spi.IgnifyrExtension`
  (registers `FileConnectorExtension`) and two for the sub-SPI (`…file.format.FileSourceFormat` →
  csv/parquet; `…file.format.FileSinkFormat` → ndjson/csv/parquet).

## Key seams
- `FileDataSourceReader` (`BaseDataSourceReader`) handles the cross-cutting bits — path resolution
  (`hdfs://` vs `FileUtils.getPath`), streaming-dir validation, the streaming `filename` log column,
  the `distinct` option — then delegates the read to `FileFormatRegistry.sourceFormat(contentType)`.
- `FileSystemWriter` (`BaseFhirWriter`) is a thin dispatcher to `FileFormatRegistry.sinkFormat(…)`.
- `FileSinkSupport` centralizes the FHIR partition-by-resource-type layout **and** the HDFS raw-text
  write path, so the enterprise Delta writer reuses identical machinery and only supplies its terminal
  write. Its `singleColumnJson` flag distinguishes ndjson (raw single column) from parquet/delta
  (parsed, partition columns injected).
- `FileConnectorExtension.initialize()` force-materializes both format registries at startup (mirrors
  `ExtensionRegistry.init()`) so a **duplicate** content-type fails fast at load — while a **missing**
  format stays lazy (a job naming it parses fine and fails only at first read/write with
  `MissingFileFormatException`, whose message names the enterprise module to install).
  `extraCapabilities` surfaces the installed format keys to the `list-plugins` command (the engine
  can't introspect the sub-registry itself).

## Tests
scalatest split (surefire disabled). **Unit** (`wildcardSuites=io.ignifyr.connector.file`, no Docker):
`FileDataSourceReaderTest`, `FileSystemWriterTest`, `FileConnectorExtensionSpec` (discovery,
`extraCapabilities`, and the missing-format install-hint UX). **Integration**
(`membersOnlySuites=io.ignifyr.integrationtest`, Docker/onFHIR): `FhirMappingJobManagerTest`. The
shared harness + fixtures come from `ignifyr-testkit` (test scope). Note: `FileSystemWriterTest`
imports `sparkSession.implicits._` (not a delta encoder) precisely so the community writer tests never
pull `delta-spark` onto the classpath.
