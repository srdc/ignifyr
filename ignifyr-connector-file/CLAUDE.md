# ignifyr-connector-file — file-system source connector

Community connector that contributes the file-system **source reader** to the engine through the
`IgnifyrExtension` SPI, and owns a connector-local **source-format sub-SPI** so the readable formats
are pluggable. Package root `io.ignifyr.connector.file`. Depends only on `ignifyr-engine`; bundled
into `ignifyr-cli` (community). Apache-2.0-clean — subject to the `ban-enterprise-deps`
maven-enforcer gate. The file-system **sink** lives in its own module, `ignifyr-sink-file`.

## The two-level SPI (the mental model)
The engine's `ExtensionRegistry` discovers this connector; the connector then runs its **own**
ServiceLoader (`FileFormatRegistry`) to discover source formats keyed by content type. **The engine
is entirely ignorant of file formats** — only this connector dispatches on source `contentType`. So
adding a format is a pure module move: the enterprise `ignifyr-format-json` (JSON/NDJSON source)
plugs in via its own `META-INF/services` file with zero changes here. The reader stays
format-agnostic. (The sink-format twin — `FileSinkFormat`/`FileSinkFormatRegistry` — lives in
`ignifyr-sink-file`.)

## Layout (`src/main/scala/io/ignifyr/connector/file/`)
- top level — `FileConnectorExtension` (the `IgnifyrExtension` impl), `FileDataSourceReader` (source).
- `format/` — the sub-SPI + plumbing: `FileSourceFormat` (+ `FileSourceReadContext`) trait;
  `FileFormatRegistry` (the connector-local ServiceLoader registry); `FileFormatHints` +
  `MissingFileFormatException` (install-hint UX).
- `format/source/` — community source handlers: `CsvSourceFormat` (csv **and** tsv, plus zip via
  `SparkUtil.readZip`), `ParquetSourceFormat`.
- `resources/META-INF/services/` — **two** files: one `io.ignifyr.engine.spi.IgnifyrExtension`
  (registers `FileConnectorExtension`) and one for the sub-SPI (`…file.format.FileSourceFormat` →
  csv/parquet).

## Key seams
- `FileDataSourceReader` (`BaseDataSourceReader`) handles the cross-cutting bits — path resolution
  (`hdfs://` vs `FileUtils.getPath`), streaming-dir validation, the streaming `filename` log column,
  the `distinct` option — then delegates the read to `FileFormatRegistry.sourceFormat(contentType)`.
- `FileConnectorExtension.initialize()` force-materializes the source-format registry at startup
  (mirrors `ExtensionRegistry.init()`) so a **duplicate** content-type fails fast at load — while a
  **missing** format stays lazy (a job naming it parses fine and fails only at first read with
  `MissingFileFormatException`, whose message names the enterprise module to install).
  `extraCapabilities` surfaces the installed source-format keys to the `list-plugins` command (the
  engine can't introspect the sub-registry itself).

## Tests
scalatest split (surefire disabled). **Unit** (`wildcardSuites=io.ignifyr.connector.file`, no Docker):
`FileDataSourceReaderTest`, `FileConnectorExtensionSpec` (discovery, `extraCapabilities`, and the
missing-format install-hint UX). **Integration** (`membersOnlySuites=io.ignifyr.integrationtest`,
Docker/onFHIR): `FhirMappingJobManagerTest` (writes through the FHIR sink, so `ignifyr-sink-fhir` is
a test-scope dep). The shared harness + fixtures come from `ignifyr-testkit` (test scope).
