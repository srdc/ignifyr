# ignifyr-connector-file — file-system source connector

Community connector that contributes the file-system **source reader** to the engine through the
`IgnifyrExtension` SPI, and owns a connector-local **source-format sub-SPI** so the readable formats
are pluggable. Package root `io.ignifyr.connector.file`. Its only compile dependency is
`ignifyr-engine` (plus test-scope `ignifyr-testkit` and `ignifyr-sink-fhir`); **both** distributions
bundle it — `ignifyr-cli` and `ignifyr-server` — since community modules ship in the enterprise server
too. Apache-2.0-clean — subject to the `ban-enterprise-deps` maven-enforcer gate. The file-system
**sink** lives in its own module, `ignifyr-sink-file`.

It carries no third-party dependency of its own (Spark's csv/parquet readers arrive with `spark-sql`,
which the engine has), so this module is not about dependency isolation: it exists so the engine ships no
concrete reader, and to be the seam that makes file formats pluggable.

## The two-level SPI (the mental model)
The engine's `ExtensionRegistry` discovers this connector; the connector then runs its **own**
ServiceLoader (`FileFormatRegistry`) to discover source formats keyed by content type. **The engine
never resolves a format handler** — only this connector dispatches on source `contentType`. So
adding a format is a pure module move: the enterprise `ignifyr-format-json` (JSON/NDJSON source)
plugs in via its own `META-INF/services` file with zero changes here. The reader stays
format-agnostic. (The sink-format twin — `FileSinkFormat`/`FileSinkFormatRegistry` — lives in
`ignifyr-sink-file`.)

Two qualifications, so you don't over-trust the seam: the engine *does* name file formats — its
`io.ignifyr.engine.model.SourceContentTypes` hardcodes `csv`/`tsv`/`parquet`/`json`/`ndjson`, including
the two whose handlers are enterprise-only — and it names this module's Maven coordinates in
`ExtensionHints`. A genuinely new content type is a pure module move only if it reuses an existing
constant or the job passes a raw string.

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
Tier-split (surefire disabled). **Short** (`wildcardSuites=io.ignifyr.connector.file`, no Docker):
`FileDataSourceReaderTest`, `FileDataSourceReaderOptionsTest`, `FileConnectorExtensionSpec` (discovery,
`extraCapabilities`, the missing-format install-hint UX, **and** the duplicate-content-type guard — the
latter asserted on `FileFormatRegistry.indexUnique`, which is `private[file]` for exactly that reason,
since ServiceLoader input can't be staged on the test classpath). **Long**
(`membersOnlySuites=io.ignifyr.integrationtest`, gated on `${skipITs}`, Docker/onFHIR):
`FhirMappingJobManagerTest` — writes through the FHIR sink, so `ignifyr-sink-fhir` is a test-scope dep.
Run it with `mvn -B verify -pl ignifyr-connector-file -DskipITs=false`. The shared harness + fixtures come
from `ignifyr-testkit` (test scope).

`FileDataSourceReaderOptionsTest` pins the two things the reader does *around* the format handler and
nobody else covers: the `distinct` read option, and path resolution — an `hdfs://` data folder is handed
through verbatim while every other path resolves against the workspace folder. That second case is
guarded deliberately: a scheme special-case on the **write** side once turned parquet output into text
(see `ignifyr-sink-file/CLAUDE.md`), so the read side's one branch is now pinned rather than assumed.
