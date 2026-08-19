#!/usr/bin/env bash
#
# Ignifyr manual end-to-end test flow.
#
# Builds the enterprise server (which bundles every plugin: streaming, scheduling, Kafka, REDCap,
# connectors, formats), stands up a self-contained Docker stack (mongo + repofyr FHIR server +
# Kafka + ignifyr-server), then exercises each behavior and verifies results in the FHIR server.
# The stack is LEFT RUNNING at the end so you can poke the tools yourself (see the printed URLs);
# pass --down to tear it down.
#
# Behaviors covered:
#   plugins    edition boundary: community CLI vs enterprise server (list-plugins)
#   batch      file (CSV) -> FHIR batch mapping
#   archive    batch with archiveMode=archive (source file moved into the archive folder)
#   streaming  folder-watch streaming (drop a CSV into a watched dir -> FHIR)
#   scheduling cron-scheduled batch (fires every minute)
#   kafka      REDCap-simulated Kafka streaming (publish records to a topic -> FHIR)
#   sql        Postgres table -> FHIR batch mapping (SQL data source)
#
# Each mapped Patient carries identifier system = the job's sourceUri, so verification is an exact
# FHIR search per behavior (no reliance on hashed ids). Jobs run through the engine CLI on the
# server's (enterprise) classpath via `docker exec`, with isolated db/checkpoint dirs so they never
# collide with the long-running REST server sharing the same workspace.
#
# Prereqs: docker + docker compose, Java-less host is fine (build runs in Maven), mvn, curl, jq.
# The build resolves onFHIR/repofyr SNAPSHOTs from Maven Central snapshots + SRDC Nexus.
#
# By default it runs TOOL-ONLY: the backend stack (mongo + repofyr + kafka + ignifyr-server) and the
# engine-CLI behavior checks — no web UI. The web UI + proxy is a VISUAL layer you opt into with
# --with-web (it also seeds real projects from data-ingestion-suite so the UI isn't empty).
#
# Usage:
#   ./run-manual-flow.sh                 # tool-only: backend stack + behavior checks, leave stack up
#   ./run-manual-flow.sh --with-web      # ALSO build+serve the web UI + proxy at http://localhost/dt4h/ignifyr
#   ./run-manual-flow.sh --no-dis        # with --with-web: don't seed the UI from data-ingestion-suite
#   ./run-manual-flow.sh --with-efk      # also run Elasticsearch+Fluentd+Kibana (Executions dashboard)
#   ./run-manual-flow.sh --skip-build    # reuse existing jars/image/web-dist
#   ./run-manual-flow.sh --only batch    # run a single behavior (plugins|batch|archive|streaming|scheduling|kafka|sql)
#   ./run-manual-flow.sh --down          # tear the stack down and exit
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE=(docker compose -f "$SCRIPT_DIR/docker-compose.test.yml")
JAR_IN_IMAGE="/usr/local/ignifyr/ignifyr-server-standalone.jar"
CONF_IN_IMAGE="/workspace/config/ignifyr-server.conf"
REPOFYR="http://localhost:8080/fhir"
IGNIFYR="http://localhost:8085/ignifyr"

SKIP_BUILD=0; ONLY=""; DOWN=0; WITH_WEB=0; WITH_DIS=1; WITH_EFK=0
while [ $# -gt 0 ]; do
  case "$1" in
    --skip-build) SKIP_BUILD=1 ;;
    --only) ONLY="${2:-}"; shift ;;
    --with-web) WITH_WEB=1 ;;
    --no-dis) WITH_DIS=0 ;;
    --with-efk) WITH_EFK=1 ;;
    --down) DOWN=1 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

# EFK is opt-in: enable the compose profile and tell the server to ship logs to fluentd.
if [ "$WITH_EFK" = "1" ]; then
  COMPOSE+=(--profile efk)
  export ITF_FLUENT_HOST=fluentd
fi

log()  { printf '\n\033[1;34m== %s ==\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m  PASS\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
fail() { printf '\033[1;31m  FAIL\033[0m %s\n' "$*"; FAILED="$FAILED\n  - $*"; FAILC=$((FAILC+1)); }
PASS=0; FAILC=0; FAILED=""

want() { [ -z "$ONLY" ] || [ "$ONLY" = "$1" ]; }

if [ "$DOWN" = "1" ]; then
  log "Tearing down the test stack"
  "${COMPOSE[@]}" down -v
  exit 0
fi

# ----- preflight ---------------------------------------------------------------
log "Preflight"
for tool in docker mvn curl jq; do
  command -v "$tool" >/dev/null 2>&1 || { echo "Missing required tool: $tool" >&2; exit 1; }
done
docker info >/dev/null 2>&1 || { echo "Docker daemon not reachable" >&2; exit 1; }
echo "  tools present, docker running"

# ----- build -------------------------------------------------------------------
if [ "$SKIP_BUILD" = "0" ]; then
  log "Building jars (server + cli) and the ignifyr-server image"
  # -DskipTests (not -Pxtest): the scalatest-maven-plugin ignores maven.test.skip, so -Pxtest alone
  # would still recompile and re-run every module's unit suite. -DskipTests is what it honors.
  ( cd "$REPO_ROOT" && mvn -q -DskipTests -pl ignifyr-server,ignifyr-cli -am package )
  ( cd "$REPO_ROOT" && docker build -q -f docker/server/Dockerfile -t srdc/ignifyr-server:latest . )
  echo "  built ignifyr-server-standalone.jar, ignifyr-engine-standalone.jar and srdc/ignifyr-server:latest"
  if [ "$WITH_EFK" = "1" ]; then
    log "Building EFK images (fluentd + kibana)"
    "${COMPOSE[@]}" build fluentd kibana
  fi
else
  log "Skipping build (--skip-build)"
fi

# ----- web build (via node Docker image; no host npm needed) ------------------
# The web Docker image can't build in-container (its file:../ editor-lib deps live outside the build
# context), and host npm may not be present. So — exactly as we built the editor libs earlier — we
# run npm inside a node:20 container with the PARENT dir mounted (so ../fhir-*-editor resolves),
# then serve the static output via the nginx proxy. Needs only Docker.
mkdir -p "$SCRIPT_DIR/web-dist"
# Build the web when --with-web, UNLESS --skip-build AND web-dist already has a built SPA. Rebuilding
# when index.html is absent prevents an empty web-dist (e.g. deleted scratch + --skip-build) from
# making nginx 500 on a redirect cycle.
if [ "$WITH_WEB" = "1" ] && { [ "$SKIP_BUILD" = "0" ] || [ ! -f "$SCRIPT_DIR/web-dist/index.html" ]; }; then
  [ "$SKIP_BUILD" = "1" ] && echo "  (--skip-build set, but web-dist has no index.html — building the web UI anyway)"
  log "Building web UI in a node:20 container (editor libs + dt4h build)"
  PARENT="$(cd "$REPO_ROOT/.." && pwd)"
  # docker -v needs a Windows-style path on Git Bash; cygpath -m yields C:/... (mixed slashes).
  if command -v cygpath >/dev/null 2>&1; then MNT="$(cygpath -m "$PARENT")"; else MNT="$PARENT"; fi
  MSYS_NO_PATHCONV=1 docker run --rm -v "$MNT:/work" -w /work node:20.19.2 bash -c '
    set -e
    (cd fhir-resource-editor   && npm install && npm run build)
    (cd fhir-expression-editor && npm install && npm run build)
    (cd ignifyr-web && npm install && npm run build:dt4h -- --base-href=/dt4h/ignifyr/)
  '
  rm -rf "${SCRIPT_DIR:?}/web-dist"/*
  cp -r "$REPO_ROOT/../ignifyr-web/dist/ignifyr-web/." "$SCRIPT_DIR/web-dist/"
  echo "  web dist copied to test-flow/web-dist"
elif [ "$WITH_WEB" = "1" ]; then
  echo "  --skip-build: reusing existing test-flow/web-dist"
fi

# ----- edition boundary via list-plugins --------------------------------------
if want plugins; then
  log "Plugin edition boundary (list-plugins)"
  CLI_JAR="$REPO_ROOT/ignifyr-cli/target/ignifyr-engine-standalone.jar"
  SRV_JAR="$REPO_ROOT/ignifyr-server/target/ignifyr-server-standalone.jar"
  if command -v java >/dev/null 2>&1 && [ -f "$CLI_JAR" ] && [ -f "$SRV_JAR" ]; then
    community="$(java -jar "$CLI_JAR" list-plugins 2>/dev/null || true)"
    enterprise="$(java -cp "$SRV_JAR" io.ignifyr.engine.Boot list-plugins 2>/dev/null || true)"
    for cap in runtime-streaming runtime-scheduling connector-kafka redcap; do
      if echo "$enterprise" | grep -qi "$cap"; then ok "enterprise bundles '$cap'"; else fail "enterprise missing '$cap'"; fi
      if echo "$community" | grep -qi "$cap"; then fail "community must NOT bundle '$cap'"; else ok "community excludes '$cap'"; fi
    done
  else
    echo "  (java or jars not available on host; run inside the container instead:"
    echo "     docker exec itf-ignifyr java -cp $JAR_IN_IMAGE io.ignifyr.engine.Boot list-plugins )"
  fi
fi

# ----- seed workspace + bring stack up ----------------------------------------
log "Seeding workspace and starting the stack"
# The engine CLI (used for the job runs below) reads a FLAT mapping/schema folder, so the fixtures
# go in cli-mappings / cli-schemas. The ignifyr-SERVER uses a project-structured repository
# (schemas/<projectId>/...), so its own repo folders are left EMPTY here — it boots clean and you
# create projects through the REST API. Putting a loose schema file in the server's schemas folder
# makes the server abort on boot.
# Remove loose files a previous (pre-fix) run may have dropped into the server's repo folders,
# which would make the server abort on boot.
rm -f "$SCRIPT_DIR"/workspace/schemas/*.json "$SCRIPT_DIR"/workspace/mappings/*.json 2>/dev/null || true
mkdir -p "$SCRIPT_DIR/workspace/cli-mappings" "$SCRIPT_DIR/workspace/cli-schemas" \
         "$SCRIPT_DIR/workspace/mappings" "$SCRIPT_DIR/workspace/schemas" \
         "$SCRIPT_DIR/workspace/mapping-jobs" "$SCRIPT_DIR/workspace/mapping-contexts" \
         "$SCRIPT_DIR/workspace/terminology-systems" \
         "$SCRIPT_DIR/workspace/archive-input" "$SCRIPT_DIR/watch/patients"
cp "$REPO_ROOT/ignifyr-testkit/src/main/resources/test-mappings/some-folder-1/patient-mapping.json" \
   "$SCRIPT_DIR/workspace/cli-mappings/patient-mapping.json"
cp "$REPO_ROOT/ignifyr-testkit/src/main/resources/test-schemas/some-folder-1/Ext-patient.StructureDefinition.json" \
   "$SCRIPT_DIR/workspace/cli-schemas/Ext-patient.StructureDefinition.json"
cp "$SCRIPT_DIR/data/patients.csv" "$SCRIPT_DIR/workspace/archive-input/patients.csv"

# Tool-only run: keep the server's project repos EMPTY so no stale DIS projects (left by an earlier
# --with-web run) get loaded. The behavior checks read the toy fixtures from cli-mappings/cli-schemas
# via -D overrides, so this does not affect them.
if [ "$WITH_WEB" = "0" ]; then
  rm -rf "$SCRIPT_DIR"/workspace/mappings/* "$SCRIPT_DIR"/workspace/schemas/* \
         "$SCRIPT_DIR"/workspace/mapping-jobs/* "$SCRIPT_DIR"/workspace/mapping-contexts/* \
         "$SCRIPT_DIR"/workspace/terminology-systems/* "$SCRIPT_DIR"/workspace/test-data/* 2>/dev/null || true
  rm -f "$SCRIPT_DIR/workspace/projects.json" 2>/dev/null || true
fi

# Populate the SERVER workspace (so the web UI shows real projects) from the data-ingestion-suite,
# which is itself a server workspace (projects.json + project-structured mappings/schemas/jobs).
# DIS is NOT modified: we copy it into the flow's workspace and rewrite only the COPIES' sink URLs
# to reach the repofyr container. Skip with --no-dis, or if the suite isn't checked out alongside.
DIS_DIR="$REPO_ROOT/../data-ingestion-suite"
if [ "$WITH_WEB" = "1" ] && [ "$WITH_DIS" = "1" ] && [ -f "$DIS_DIR/projects.json" ]; then
  log "Seeding server workspace from data-ingestion-suite (real DT4H projects; DIS left untouched)"
  for d in mappings schemas mapping-jobs mapping-contexts terminology-systems test-data; do
    if [ -d "$DIS_DIR/$d" ]; then
      rm -rf "${SCRIPT_DIR:?}/workspace/$d"
      cp -r "$DIS_DIR/$d" "$SCRIPT_DIR/workspace/$d"
    fi
  done
  cp "$DIS_DIR/projects.json" "$SCRIPT_DIR/workspace/projects.json"
  # Copies only: point every job's FHIR sink at the repofyr container instead of localhost/onfhir.
  find "$SCRIPT_DIR/workspace/mapping-jobs" -name '*.json' -exec \
    sed -i -e 's#http://localhost:8080/fhir#http://repofyr:8080/fhir#g' \
           -e 's#http://onfhir:8080/fhir#http://repofyr:8080/fhir#g' {} +
  echo "  server workspace populated from DIS; job sinks point at repofyr"
elif [ "$WITH_WEB" = "1" ] && [ "$WITH_DIS" = "1" ]; then
  echo "  (data-ingestion-suite not found next to the repo; UI starts empty — use --no-dis to silence)"
fi

# Start EFK first (if enabled) so fluentd is listening by the time the server ships its first logs.
if [ "$WITH_EFK" = "1" ]; then
  log "Starting EFK (Elasticsearch + Fluentd + Kibana)"
  "${COMPOSE[@]}" up -d elasticsearch fluentd kibana
fi

"${COMPOSE[@]}" up -d --wait mongo repofyr kafka postgres ignifyr

# Wait for the FHIR server and the ignifyr REST server to answer.
log "Waiting for repofyr and ignifyr-server"
for i in $(seq 1 30); do curl -fs "$REPOFYR/metadata" >/dev/null 2>&1 && break || sleep 4; done
curl -fs "$REPOFYR/metadata" >/dev/null 2>&1 && echo "  repofyr up at $REPOFYR" || { echo "repofyr did not come up" >&2; exit 1; }
for i in $(seq 1 30); do curl -fs -X OPTIONS "$IGNIFYR/projects" >/dev/null 2>&1 && break || sleep 4; done
echo "  ignifyr-server up at $IGNIFYR"

# Web UI + proxy (serves the host-built SPA and proxies the API — same origin, no CORS).
if [ "$WITH_WEB" = "1" ]; then
  log "Starting web UI + proxy"
  "${COMPOSE[@]}" up -d proxy
  echo "  web UI at http://localhost/dt4h/ignifyr"
fi

# ----- helpers -----------------------------------------------------------------
# Run a job file through the engine CLI on the enterprise classpath, isolated db/checkpoint.
# The checkpoint and db for this job are wiped first
run_job() {
  local name="$1" job="$2"; shift 2
  docker exec "$@" itf-ignifyr sh -c \
    "rm -rf /workspace/clichk/$name /workspace/clidb/$name; \
     java -Dconfig.file=$CONF_IN_IMAGE \
     -Dignifyr.mappings.repository.folder-path=/workspace/cli-mappings \
     -Dignifyr.mappings.schemas.repository.folder-path=/workspace/cli-schemas \
     -Dignifyr.db-path=/workspace/clidb/$name -Dspark.checkpoint-dir=/workspace/clichk/$name \
     -cp $JAR_IN_IMAGE io.ignifyr.engine.Boot run --job /workspace/config/jobs/$job"
}

# FHIR count of Patients with identifier <system>|<value>. Echoes an integer.
patient_count() {
  local system="$1" value="$2"
  curl -fs "$REPOFYR/Patient?identifier=$(printf '%s' "$system|$value" | jq -sRr @uri)&_summary=count" \
    | jq -r '.total // 0'
}

# Poll patient_count until >=1 (or timeout seconds). Returns 0 on success.
await_patient() {
  local system="$1" value="$2" timeout="${3:-90}" waited=0
  while [ "$waited" -lt "$timeout" ]; do
    [ "$(patient_count "$system" "$value")" -ge 1 ] 2>/dev/null && return 0
    sleep 5; waited=$((waited+5))
  done
  return 1
}

# ----- batch -------------------------------------------------------------------
if want batch; then
  log "Batch file -> FHIR"
  run_job batch batch-file-job.json
  if await_patient "https://ignifyr.io/test-flow/batch" "p1" 30; then ok "batch produced Patient p1"; else fail "batch: Patient p1 not found in repofyr"; fi
fi

# ----- archiving ---------------------------------------------------------------
if want archive; then
  log "Batch with archiving (archiveMode=archive)"
  cp "$SCRIPT_DIR/data/patients.csv" "$SCRIPT_DIR/workspace/archive-input/patients.csv"
  run_job archive archive-file-job.json
  if await_patient "https://ignifyr.io/test-flow/archive" "p1" 30; then ok "archive job produced Patient p1"; else fail "archive: Patient p1 not found"; fi
  # Source file should have been moved out of archive-input and into the archive-folder.
  if docker exec itf-ignifyr sh -c 'test ! -f /workspace/archive-input/patients.csv'; then ok "source file removed from input folder"; else fail "source file still present in input folder"; fi
  if docker exec itf-ignifyr sh -c 'find /workspace/archive-folder -name patients.csv | grep -q .'; then ok "source file archived under archive-folder"; else fail "source file not found under archive-folder"; fi
fi

# ----- folder-watch streaming --------------------------------------------------
if want streaming; then
  log "Folder-watch streaming (drop a CSV into the watched dir)"
  rm -f "$SCRIPT_DIR/watch/patients/"*.csv 2>/dev/null || true
  # Start the streaming job (self-terminates after 100s via `timeout`), detached from this script.
  run_job streaming streaming-watch-job.json -d
  sleep 40   # let the streaming query initialise before dropping the file
  cp "$SCRIPT_DIR/data/stream-patients.csv" "$SCRIPT_DIR/watch/patients/stream-patients.csv"
  if await_patient "https://ignifyr.io/test-flow/stream" "sp1" 180; then ok "streaming processed the dropped file (Patient sp1)"; else fail "streaming: Patient sp1 not found after drop"; fi
  docker exec itf-ignifyr sh -c "pkill -f streaming-watch-job || true" >/dev/null 2>&1 || true
fi

# ----- scheduling --------------------------------------------------------------
if want scheduling; then
  log "Cron scheduling (fires every minute)"
  run_job scheduling scheduling-file-job.json -d
  echo "  scheduled; waiting up to 100s for the first cron fire..."
  if await_patient "https://ignifyr.io/test-flow/scheduled" "p1" 100; then ok "scheduler fired and produced Patient p1"; else fail "scheduling: no scheduled run observed within 100s"; fi
  docker exec itf-ignifyr sh -c "pkill -f scheduling-file-job || true" >/dev/null 2>&1 || true
fi

# ----- Kafka (REDCap-simulated) ------------------------------------------------
if want kafka; then
  log "Kafka streaming (REDCap simulated via raw Kafka)"
  MSYS_NO_PATHCONV=1 docker exec itf-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic redcap-patients >/dev/null 2>&1 || true
  sleep 5
  # Publish REDCap-shaped records to the (fresh) topic through the broker container's console producer.
  MSYS_NO_PATHCONV=1 docker exec -i itf-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic redcap-patients \
    < "$SCRIPT_DIR/data/redcap-patients.ndjson" || fail "could not publish to Kafka"
  run_job kafka kafka-redcap-job.json -d
  if await_patient "https://ignifyr.io/test-flow/redcap-kafka" "rp1" 180; then ok "Kafka streaming consumed topic (Patient rp1)"; else fail "kafka: Patient rp1 not found"; fi
  docker exec itf-ignifyr sh -c "pkill -f kafka-redcap-job || true" >/dev/null 2>&1 || true
fi

# ----- sql ----------------------------------------------------------
if want sql; then
  log "SQL (Postgres) -> FHIR"
  run_job sql sql-patient-job.json
  if await_patient "https://ignifyr.io/test-flow/sql" "p1" 60; then ok "sql read the Postgres table and produced Patient p1"; else fail "sql: Patient p1 not found in repofyr"; fi
fi

# ----- summary -----------------------------------------------------------------
log "Summary"
printf '  passed: %s   failed: %s\n' "$PASS" "$FAILC"
[ "$FAILC" -eq 0 ] || printf '  failures:%b\n' "$FAILED"
[ "$WITH_WEB" = "1" ] && WEB_LINE="  Web UI       : http://localhost/dt4h/ignifyr" || WEB_LINE="  Web UI       : (tool-only run; add --with-web for the visual UI)"
[ "$WITH_EFK" = "1" ] && EFK_LINE="  Kibana       : http://localhost/dt4h/ignifyr/kibana (Executions dashboard; give ES ~1 min to be ready)" || EFK_LINE="  Kibana       : (disabled; add --with-efk for the Executions dashboard)"
cat <<EOF

Stack is still running so you can test the tools yourself:
$WEB_LINE
$EFK_LINE
  Ignifyr REST : $IGNIFYR         (e.g. curl $IGNIFYR/projects)
  Repofyr FHIR : $REPOFYR   (e.g. curl "$REPOFYR/Patient?_summary=count")
  Kafka broker : localhost:9092
  Postgres     : host=postgres port=5432 db=ignifyr user=ignifyr pass=ignifyr (from the server; table 'patients')
  list-plugins : docker exec itf-ignifyr java -cp $JAR_IN_IMAGE io.ignifyr.engine.Boot list-plugins
Tear down with:  $0 --down
EOF
[ "$FAILC" -eq 0 ]
