#!/usr/bin/env bash
#
# Enterprise/Community separation check: the maven-enforcer "ban-enterprise-deps" gate.
#
# The whole split relies on community modules being unable to depend on enterprise-only libraries
# (Kafka, cron4j, Delta, DB2 JCC, Logstash/Fluentd). This test PROVES the gate actually fails the
# build: it temporarily injects a banned dependency (cron4j) into a community module's pom, runs
# `mvn validate`, and asserts the enforcer rejects it — then restores the pom.
#
# Usage: test-flow/check-enforcer-gate.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MODULE="ignifyr-connector-sql"           # a community module that opts into the ban gate
POM="$REPO_ROOT/$MODULE/pom.xml"
BAK="$POM.enforcer-test.bak"

command -v mvn >/dev/null 2>&1 || { echo "mvn required" >&2; exit 1; }
grep -q 'ban-enterprise-deps' "$POM" || { echo "$MODULE does not opt into the ban gate; pick another module" >&2; exit 1; }

restore() { [ -f "$BAK" ] && mv -f "$BAK" "$POM"; }
trap restore EXIT INT TERM

echo "== Injecting a banned dependency (cron4j) into $MODULE =="
cp "$POM" "$BAK"
# Insert a single-line cron4j dependency right after the first <dependencies> element.
awk '
  /<dependencies>/ && !done {
    print
    print "        <dependency><groupId>it.sauronsoftware.cron4j</groupId><artifactId>cron4j</artifactId></dependency>"
    done = 1
    next
  }
  { print }
' "$BAK" > "$POM"

echo "== Running mvn validate (the enforcer runs in the validate phase) =="
out="$( cd "$REPO_ROOT" && mvn -q -pl "$MODULE" -am validate 2>&1 )"
status=$?

echo "-- maven exit status: $status"
if [ "$status" -ne 0 ] && echo "$out" | grep -qiE 'BannedDependencies|enterprise-only|cron4j'; then
  printf '\033[1;32m  PASS\033[0m the enforcer gate rejected the banned dependency\n'
  echo "$out" | grep -iE 'enterprise-only|BannedDependencies|cron4j' | head -3 | sed 's/^/       /'
  result=0
else
  printf '\033[1;31m  FAIL\033[0m the build did NOT fail on the banned dependency (gate not enforced!)\n'
  echo "$out" | tail -15 | sed 's/^/       /'
  result=1
fi

# restore happens via the EXIT trap
exit "$result"
