#!/usr/bin/env bash
#
# Ignifyr automated tests — two standard tiers.
#
# Follows the usual Maven split (Surefire-style unit @ `test`, Failsafe-style integration @ `verify`):
#
#   --short   Unit tests only  ->  `mvn test`.   Fast, NO Docker. Quick-feedback / smoke run.
#   --long    Full verification ->  `mvn -B verify -DskipITs=false` (unit + integration via
#             TestContainers: MongoDB + srdc/onfhir:r5 + Kafka) PLUS the tier gate and the
#             packaged edition checks. DOCKER REQUIRED.
#
#             The long tier is opt-in: the root pom defaults `skipITs` to true, so a plain
#             `mvn test` / `mvn package` / `mvn install` stays on the short tier. `-DskipITs=false`
#             is what turns the integration executions on.
#
#   --behavior X   Run one area only: streaming | scheduling | kafka | archiving | connectors |
#                  sinks | endpoints | editions.
#
# The live end-to-end tier is a separate command (it builds images and stands up a real stack):
#   test-flow/run-manual-flow.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

usage() { grep '^#' "$0" | sed 's/^# \{0,1\}//'; }
log()   { printf '\n\033[1;34m== %s ==\033[0m\n' "$*"; }

MODE=""; BEHAVIOR=""
while [ $# -gt 0 ]; do
  case "$1" in
    --short|--unit-only) MODE="short" ;;
    --long|--it-only|--full) MODE="long" ;;
    --behavior)  MODE="behavior"; BEHAVIOR="${2:-}"; shift ;;
    -h|--help)   usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; echo; usage; exit 2 ;;
  esac
  shift
done

command -v mvn >/dev/null 2>&1 || { echo "mvn not found" >&2; exit 1; }

case "$MODE" in
  short)
    log "SHORT tier — unit tests (no Docker)"
    mvn -B test
    ;;
  long)
    log "LONG tier — full verification (Docker required)"
    docker info >/dev/null 2>&1 || { echo "Docker must be running for the integration tier" >&2; exit 1; }
    log "Test-tier integrity (no container-backed suite hiding in the short tier)"
    bash "$SCRIPT_DIR/check-test-tiers.sh"
    log "Unit + integration tests (TestContainers: Mongo + onFHIR + Kafka)"
    mvn -B verify -DskipITs=false
    log "Edition separation — packaged jars + SPI + community-CLI behavior"
    bash "$SCRIPT_DIR/check-editions.sh"
    log "Edition separation — banned-dependency enforcer gate"
    bash "$SCRIPT_DIR/check-enforcer-gate.sh"
    ;;
  behavior)
    docker info >/dev/null 2>&1 || echo "WARNING: Docker not detected; integration suites will fail."
    case "$BEHAVIOR" in
      streaming)  log "runtime-streaming (unit + folder-watch & Kafka E2E)"; mvn -B -pl ignifyr-runtime-streaming -am verify -DskipITs=false ;;
      scheduling) log "runtime-scheduling (unit + cron/SQL E2E)";            mvn -B -pl ignifyr-runtime-scheduling -am verify -DskipITs=false ;;
      kafka)      log "connector-kafka + runtime-streaming Kafka E2E";       mvn -B -pl ignifyr-connector-kafka,ignifyr-runtime-streaming -am verify -DskipITs=false ;;
      archiving)  log "engine archiving unit suite (no Docker)";             mvn -B -pl ignifyr-engine -am test -Dsuffixes='.*FileStreamInputArchiverTest' ;;
      connectors) log "file + sql connectors (unit + integration)";         mvn -B -pl ignifyr-connector-file,ignifyr-connector-sql -am verify -DskipITs=false ;;
      sinks)      log "sink modules (fhir + file + omop registration/writer specs)"; mvn -B -pl ignifyr-sink-fhir,ignifyr-sink-file,ignifyr-sink-omop -am test ;;
      endpoints)  log "server REST endpoints (integration)";                mvn -B -pl ignifyr-server -am verify -DskipITs=false ;;
      editions)   log "edition separation (community registry spec + jar/SPI content + enforcer gate)"
                  mvn -B -pl ignifyr-cli -am test
                  bash "$SCRIPT_DIR/check-test-tiers.sh"
                  bash "$SCRIPT_DIR/check-editions.sh"
                  bash "$SCRIPT_DIR/check-enforcer-gate.sh" ;;
      *) echo "Unknown behavior '$BEHAVIOR' (streaming|scheduling|kafka|archiving|connectors|sinks|endpoints|editions)" >&2; exit 2 ;;
    esac
    ;;
  "")
    usage; exit 0
    ;;
esac

log "Done"
