#!/usr/bin/env bash
#
# Ignifyr automated test orchestration.
#
# Drives the module unit + integration suites (existing coverage plus the two streaming E2E
# tests) with the right Maven phases. Unit suites need no Docker; integration suites spin up
# TestContainers (MongoDB + srdc/onfhir:r5, and a Kafka broker for the Kafka streaming test), so
# Docker Desktop must be running for `verify`.
#
# Usage:
#   ./run-automated-tests.sh                 # build + all unit tests + all integration tests (mvn -B verify)
#   ./run-automated-tests.sh --unit-only     # build + unit tests only (fast, no Docker)
#   ./run-automated-tests.sh --behavior X    # one behavior: streaming | scheduling | archiving | kafka | connectors | sinks | endpoints | editions
#
# Behavior -> suite map (also see test-flow/README.md coverage matrix):
#   streaming   ignifyr-runtime-streaming  : StreamingSinkHandlerTest (unit) + StreamingFolderWatchTest,
#                                            KafkaStreamingRedcapTest (integration)
#   scheduling  ignifyr-runtime-scheduling : SchedulingRuntimeExtensionSpec (unit) + SchedulingTest (integration)
#   archiving   ignifyr-engine             : FileStreamInputArchiverTest (unit, no Docker)
#   kafka       ignifyr-connector-kafka + ignifyr-runtime-streaming : KafkaConnectorExtensionSpec + KafkaStreamingRedcapTest
#   connectors  ignifyr-connector-file/-sql: reader specs (unit) + FhirMappingJobManagerTest (integration)
#   endpoints   ignifyr-server             : BaseEndpointTest-based endpoint suites (integration)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

MODE="all"; BEHAVIOR=""
while [ $# -gt 0 ]; do
  case "$1" in
    --unit-only) MODE="unit" ;;
    --it-only)   MODE="it" ;;
    --behavior)  MODE="behavior"; BEHAVIOR="${2:-}"; shift ;;
    -h|--help)   grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$*"; }

command -v mvn >/dev/null 2>&1 || { echo "mvn not found" >&2; exit 1; }

case "$MODE" in
  unit)
    log "Build (skip tests)"; mvn -q -DskipTests install
    log "Unit tests (no Docker)";  mvn -q test
    ;;
  it)
    docker info >/dev/null 2>&1 || { echo "Docker required for integration tests" >&2; exit 1; }
    log "Full verify (unit + integration; TestContainers)"; mvn -B verify
    ;;
  behavior)
    docker info >/dev/null 2>&1 || echo "WARNING: Docker not detected; integration suites will fail."
    case "$BEHAVIOR" in
      streaming)  log "runtime-streaming (unit + folder-watch & Kafka E2E)"; mvn -B -pl ignifyr-runtime-streaming -am verify ;;
      scheduling) log "runtime-scheduling (unit + cron/SQL E2E)";            mvn -B -pl ignifyr-runtime-scheduling -am verify ;;
      kafka)      log "connector-kafka + runtime-streaming Kafka E2E";       mvn -B -pl ignifyr-connector-kafka,ignifyr-runtime-streaming -am verify ;;
      archiving)  log "engine archiving unit suite (no Docker)";             mvn -q -pl ignifyr-engine -am test -DwildcardSuites=io.ignifyr.test.engine.execution.FileStreamInputArchiverTest ;;
      connectors) log "file + sql connectors (unit + integration)";         mvn -B -pl ignifyr-connector-file,ignifyr-connector-sql -am verify ;;
      sinks)      log "sink modules (fhir + file + omop registration/writer specs)"; mvn -B -pl ignifyr-sink-fhir,ignifyr-sink-file,ignifyr-sink-omop -am test ;;
      endpoints)  log "server REST endpoints (integration)";                mvn -B -pl ignifyr-server -am verify ;;
      editions)   log "edition separation (community registry spec + jar/SPI content + enforcer gate)"
                  mvn -B -pl ignifyr-cli -am test                 # CommunityEditionSeparationSpec (registry on community classpath)
                  "$SCRIPT_DIR/check-editions.sh"                  # jar content + SPI manifest + community-CLI refusal behavior
                  "$SCRIPT_DIR/check-enforcer-gate.sh" ;;          # proves the banned-dependency gate fails the build
      *) echo "Unknown behavior '$BEHAVIOR' (streaming|scheduling|kafka|archiving|connectors|endpoints|editions)" >&2; exit 2 ;;
    esac
    ;;
  all)
    log "Build (skip tests)";                       mvn -q -DskipTests install
    log "Unit tests (no Docker)";                   mvn -q test
    docker info >/dev/null 2>&1 || { echo "Docker required for the integration phase" >&2; exit 1; }
    log "Integration tests (TestContainers: Mongo + onFHIR + Kafka)"; mvn -B verify
    ;;
esac

log "Done"
