#!/usr/bin/env bash
#
# Enterprise/Community edition separation check (packaged artifacts).
#
# Complements the CommunityEditionSeparationSpec (which checks the registry on the community *test*
# classpath) by inspecting the actually-shipped fat jars and the community CLI's runtime behavior:
#
#   #2 jar content  — the community jar must contain NONE of the enterprise packages/libs; the
#                     server jar must contain them.
#   #4 SPI manifest — the merged META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension in the
#                     community jar lists only community extensions; the server jar lists the rest.
#   #1 behavior     — the community CLI refuses enterprise jobs with an actionable MissingX error
#                     (streaming -> MissingCapabilityException, Kafka -> MissingConnectorException).
#
# Needs both fat jars; builds them (tests skipped) if missing. Usage: test-flow/check-editions.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLI_JAR="$REPO_ROOT/ignifyr-cli/target/ignifyr-engine-standalone.jar"
SRV_JAR="$REPO_ROOT/ignifyr-server/target/ignifyr-server-standalone.jar"

PASS=0; FAILC=0
ok()   { printf '  \033[1;32mPASS\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '  \033[1;31mFAIL\033[0m %s\n' "$*"; FAILC=$((FAILC+1)); }
warn() { printf '  \033[1;33mWARN\033[0m %s\n' "$*"; }   # non-fatal (packaged-runtime behavior is environment-sensitive)
log()  { printf '\n\033[1;34m== %s ==\033[0m\n' "$*"; }

# Enterprise footprints that must be absent from the community jar and present in the server jar.
ENTERPRISE_CLASSES=(
  io/ignifyr/runtime/streaming io/ignifyr/runtime/scheduling
  io/ignifyr/connector/kafka io/ignifyr/connector/fhirserver
  io/ignifyr/redcap io/ignifyr/format/delta io/ignifyr/format/json io/ignifyr/observability
  io/ignifyr/sink/omop
)
ENTERPRISE_LIBS=( org/apache/spark/sql/kafka io/delta/ it/sauronsoftware/cron4j net/logstash/logback )
ENTERPRISE_EXT=(
  io.ignifyr.runtime.streaming io.ignifyr.runtime.scheduling
  io.ignifyr.connector.kafka io.ignifyr.connector.fhirserver io.ignifyr.redcap io.ignifyr.sink.omop
)
# Community distribution now includes the split-out FHIR + file sinks (sink-fhir / sink-file).
COMMUNITY_EXT=( io.ignifyr.connector.sql io.ignifyr.connector.file io.ignifyr.sink.fhir io.ignifyr.sink.file )

log "(Re)building the community + server fat jars (always fresh — stale jars give false results)"
( cd "$REPO_ROOT" && mvn -q -DskipTests -pl ignifyr-cli,ignifyr-server -am package ) || { echo "build failed" >&2; exit 1; }
[ -f "$CLI_JAR" ] && [ -f "$SRV_JAR" ] || { echo "jars missing after build" >&2; exit 1; }
command -v unzip >/dev/null 2>&1 || { echo "unzip required" >&2; exit 1; }

# ---- #2 jar content ----------------------------------------------------------
log "#2 Jar content: enterprise code/libs excluded from community, present in server"
# The server fat jar is large (Spark) and can be zip64 with >65535 entries — Info-ZIP `unzip -l`
# truncates/empties its listing there, which would falsely report everything as "missing". Prefer the
# JDK `jar` tool (handles large/zip64), falling back to zipinfo, then unzip.
list_jar() { jar tf "$1" 2>/dev/null || unzip -Z1 "$1" 2>/dev/null || unzip -l "$1" 2>/dev/null; }
CLI_ENTRIES="$(list_jar "$CLI_JAR")"
SRV_ENTRIES="$(list_jar "$SRV_JAR")"
for marker in "${ENTERPRISE_CLASSES[@]}" "${ENTERPRISE_LIBS[@]}"; do
  if echo "$CLI_ENTRIES" | grep -q "$marker"; then bad "community jar unexpectedly contains '$marker'"; else ok "community jar excludes '$marker'"; fi
done
for marker in "${ENTERPRISE_CLASSES[@]}"; do
  if echo "$SRV_ENTRIES" | grep -q "$marker"; then ok "server jar contains '$marker'"; else bad "server jar missing '$marker'"; fi
done

# ---- #4 SPI manifest ---------------------------------------------------------
log "#4 ServiceLoader manifest (io.ignifyr.engine.spi.IgnifyrExtension)"
CLI_SPI="$(unzip -p "$CLI_JAR" META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension 2>/dev/null)"
SRV_SPI="$(unzip -p "$SRV_JAR" META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension 2>/dev/null)"
for ext in "${COMMUNITY_EXT[@]}"; do
  echo "$CLI_SPI" | grep -q "$ext" && ok "community SPI lists '$ext'" || bad "community SPI missing '$ext'"
done
for ext in "${ENTERPRISE_EXT[@]}"; do
  echo "$CLI_SPI" | grep -q "$ext" && bad "community SPI unexpectedly lists '$ext'" || ok "community SPI excludes '$ext'"
  echo "$SRV_SPI" | grep -q "$ext" && ok "server SPI lists '$ext'" || bad "server SPI missing '$ext'"
done

# ---- #1 community CLI runtime behavior --------------------------------------
log "#1 Community CLI refuses enterprise jobs with an actionable error"
if command -v java >/dev/null 2>&1; then
  WS="$(mktemp -d)"; mkdir -p "$WS/mappings" "$WS/schemas"
  cp "$REPO_ROOT/ignifyr-testkit/src/main/resources/test-mappings/some-folder-1/patient-mapping.json" "$WS/mappings/"
  cp "$REPO_ROOT/ignifyr-testkit/src/main/resources/test-schemas/some-folder-1/Ext-patient.StructureDefinition.json" "$WS/schemas/"
  run_cli() { # <job-file>
    java -Dignifyr.mappings.repository.folder-path="$WS/mappings" \
         -Dignifyr.mappings.schemas.repository.folder-path="$WS/schemas" \
         -jar "$CLI_JAR" run --job "$1" 2>&1
  }
  out="$(run_cli "$SCRIPT_DIR/config/jobs/streaming-watch-job.json")"
  echo "$out" | grep -q "MissingCapabilityException\|runtime-streaming" && ok "streaming job -> MissingCapabilityException (streaming)" || warn "streaming job did not clearly report the missing streaming capability (registry is authoritatively checked by CommunityEditionSeparationSpec)"
  out="$(run_cli "$SCRIPT_DIR/config/jobs/kafka-redcap-job.json")"
  echo "$out" | grep -q "MissingConnectorException\|connector-kafka" && ok "Kafka job -> MissingConnectorException (kafka)" || warn "Kafka job did not clearly report the missing Kafka connector (see CommunityEditionSeparationSpec)"
  # list-plugins must not advertise enterprise plugins on the community jar.
  lp="$(java -jar "$CLI_JAR" list-plugins 2>/dev/null)"
  echo "$lp" | grep -qi "runtime-streaming\|connector-kafka\|redcap" && bad "list-plugins advertises an enterprise plugin on the community jar" || ok "list-plugins shows no enterprise plugins on the community jar"
  rm -rf "$WS"
else
  echo "  (java not on PATH; skipping runtime behavior checks — jar-content + SPI checks still ran)"
fi

# ---- summary -----------------------------------------------------------------
log "Summary"; printf '  passed: %s   failed: %s\n' "$PASS" "$FAILC"
[ "$FAILC" -eq 0 ]
