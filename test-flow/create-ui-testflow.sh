#!/usr/bin/env bash
#
# Create a "test-flow" project in the RUNNING ignifyr-server via its REST API, so a SQL job (Postgres
# 'patients' table) and a Kafka job (redcap-patients topic) show up in the web UI and can be run and
# observed from there. Idempotent-ish: re-running reports 409 (already exists) and continues.
#
# Prereq: the manual stack is up (test-flow/run-manual-flow.sh --with-web [--with-efk]) so the server,
# Postgres and Kafka containers are running. Needs curl.
#
# Usage: test-flow/create-ui-testflow.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BASE="http://localhost:8085/ignifyr"
PROJECT="test-flow"

SCHEMA_FILE="$REPO_ROOT/ignifyr-testkit/src/main/resources/test-schemas/some-folder-1/Ext-patient.StructureDefinition.json"
MAPPING_FILE="$REPO_ROOT/ignifyr-testkit/src/main/resources/test-mappings/some-folder-1/patient-mapping.json"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
FAILC=0

command -v curl >/dev/null 2>&1 || { echo "curl required" >&2; exit 1; }
curl -fsS -X OPTIONS "$BASE/projects" >/dev/null 2>&1 || { echo "ignifyr-server not reachable at $BASE — bring the stack up first" >&2; exit 1; }

# POST <url> <file> <label>: print HTTP status + a short slice of the body. A non-2xx/409 response is counted in FAILC for return value control
post() {
  local url="$1" file="$2" label="$3" code
  code="$(curl -sS -o "$TMP/resp" -w '%{http_code}' -H 'Content-Type: application/json' -X POST --data-binary @"$file" "$url")"
  case "$code" in
    2*)  printf '  \033[1;32mOK  %s\033[0m (%s)\n' "$label" "$code" ;;
    409) printf '  \033[1;33mEXISTS %s\033[0m (409 — leaving as-is)\n' "$label" ;;
    *)   printf '  \033[1;31mFAIL %s\033[0m (%s): %s\n' "$label" "$code" "$(head -c 300 "$TMP/resp")"; FAILC=$((FAILC+1)) ;;
  esac
}

echo "== 1/5 create project '$PROJECT' =="
cat > "$TMP/project.json" <<JSON
{
  "id": "$PROJECT",
  "name": "$PROJECT",
  "description": "Manual SQL + Kafka source tests (Postgres 'patients' table + 'redcap-patients' topic).",
  "schemaUrlPrefix": "https://aiccelerate.eu/fhir/StructureDefinition/",
  "mappingUrlPrefix": "https://aiccelerate.eu/fhir/mappings/",
  "schemas": [],
  "mappings": []
}
JSON
post "$BASE/projects" "$TMP/project.json" "project"

echo "== 2/5 create Patient schema (Ext-patient StructureDefinition) =="
post "$BASE/projects/$PROJECT/schemas?format=StructureDefinition" "$SCHEMA_FILE" "schema Ext-patient"

echo "== 3/5 create patient mapping =="
post "$BASE/projects/$PROJECT/mappings" "$MAPPING_FILE" "mapping patient-mapping"

echo "== 4/5 create SQL job (Postgres 'patients' -> FHIR) =="
cat > "$TMP/sql-job.json" <<'JSON'
{
  "id": "sql-patient-job",
  "name": "sql-patient-job",
  "sourceSettings": {
    "sql": {
      "jsonClass": "SqlSourceSettings",
      "name": "sql",
      "sourceUri": "https://ignifyr.io/test-flow/sql",
      "databaseUrl": "jdbc:postgresql://postgres:5432/ignifyr",
      "username": "ignifyr",
      "password": "ignifyr"
    }
  },
  "sinkSettings": {
    "jsonClass": "FhirRepositorySinkSettings",
    "fhirRepoUrl": "http://repofyr:8080/fhir"
  },
  "mappings": [
    {
      "name": "patient-mapping",
      "mappingRef": "https://aiccelerate.eu/fhir/mappings/patient-mapping",
      "sourceBinding": {
        "source": {
          "jsonClass": "SqlSource",
          "tableName": "patients",
          "sourceRef": "sql"
        }
      }
    }
  ],
  "dataProcessingSettings": { "saveErroneousRecords": false, "archiveMode": "off" }
}
JSON
post "$BASE/projects/$PROJECT/jobs" "$TMP/sql-job.json" "job sql-patient-job"

echo "== 5/5 create Kafka job (redcap-patients topic -> FHIR) =="
cat > "$TMP/kafka-job.json" <<'JSON'
{
  "id": "kafka-redcap-job",
  "name": "kafka-redcap-job",
  "sourceSettings": {
    "kafka": {
      "jsonClass": "KafkaSourceSettings",
      "name": "kafka",
      "sourceUri": "https://ignifyr.io/test-flow/redcap-kafka",
      "bootstrapServers": "kafka:9092",
      "asStream": true
    }
  },
  "sinkSettings": {
    "jsonClass": "FhirRepositorySinkSettings",
    "fhirRepoUrl": "http://repofyr:8080/fhir"
  },
  "mappings": [
    {
      "name": "patient-mapping",
      "mappingRef": "https://aiccelerate.eu/fhir/mappings/patient-mapping",
      "sourceBinding": {
        "source": {
          "jsonClass": "KafkaSource",
          "topicName": "redcap-patients",
          "options": { "startingOffsets": "earliest" },
          "sourceRef": "kafka"
        }
      }
    }
  ],
  "dataProcessingSettings": { "saveErroneousRecords": false, "archiveMode": "off" }
}
JSON
post "$BASE/projects/$PROJECT/jobs" "$TMP/kafka-job.json" "job kafka-redcap-job"

echo
if [ "$FAILC" -ne 0 ]; then
  printf '\033[1;31m%s create(s) failed — see the FAIL lines above.\033[0m\n' "$FAILC"
  exit 1
fi

# Seed the Kafka topic so 'kafka-redcap-job' has data
echo "== seeding the 'redcap-patients' topic (reset + publish once) =="
if MSYS_NO_PATHCONV=1 docker exec itf-kafka true 2>/dev/null; then
  MSYS_NO_PATHCONV=1 docker exec itf-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic redcap-patients >/dev/null 2>&1 || true
  sleep 5
  if MSYS_NO_PATHCONV=1 docker exec -i itf-kafka /opt/kafka/bin/kafka-console-producer.sh \
       --bootstrap-server localhost:9092 --topic redcap-patients < "$SCRIPT_DIR/data/redcap-patients.ndjson" 2>/dev/null; then
    printf '  \033[1;32mOK  topic seeded with 3 records\033[0m\n'
  else
    printf '  \033[1;33mWARN could not publish to the topic - seed it manually before running the Kafka job\033[0m\n'
  fi
else
  printf '  \033[1;33mWARN itf-kafka not running - start the stack, then seed the topic before the Kafka job\033[0m\n'
fi

echo
echo "Done. Open the web UI -> project '$PROJECT' -> Jobs:"
echo "  http://localhost/dt4h/ignifyr"
echo "Run 'sql-patient-job' (batch) and 'kafka-redcap-job' (streaming) - both are ready to run."
