#!/usr/bin/env bash
#
# Test-tier integrity gate.
#
# The short/long split is only real if it is ENFORCED, not merely conventional. Ignifyr decides a
# suite's tier by its package:
#
#   SHORT (`test` phase)               -> the module's own package, pinned via <wildcardSuites>
#   LONG  (`integration-test` phase)   -> `io.ignifyr.integrationtest`, via <membersOnlySuites>,
#                                          gated behind the root-pom `skipITs` property
#
# Nothing in Maven or ScalaTest stops someone dropping a Testcontainers-backed suite into a
# short-tier package (that is exactly how ignifyr-server's three onFHIR suites ended up running in
# `mvn test`). This script fails the build when that happens. Three invariants:
#
#   1. Every test suite that touches a container declares `package io.ignifyr.integrationtest`.
#   2. No module declares a bare <scalatest-maven-plugin> (a bare block runs EVERY suite at `test`).
#   3. Every module that owns an `io.ignifyr.integrationtest` suite has an `integration-test`
#      execution wired to `${skipITs}`, and vice versa.
#
# Usage: test-flow/check-test-tiers.sh    (no Docker, no Maven — pure source inspection)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

INTEGRATION_PKG="io.ignifyr.integrationtest"
# Anything that pulls a container up. OnFhirTestContainer is listed because its `onFhirClient` val
# starts MongoDB + onFHIR at suite *construction* time, so ScalaTest tags cannot defer it.
CONTAINER_RE='Testcontainers|OnFhirTestContainer|GenericContainer|MongoDBContainer|KafkaContainer|PostgreSQLContainer|DockerImageName|DockerComposeContainer'

FAILC=0
ok()   { printf '  \033[1;32mPASS\033[0m %s\n' "$*"; }
bad()  { printf '  \033[1;31mFAIL\033[0m %s\n' "$*"; FAILC=$((FAILC+1)); }
warn() { printf '  \033[1;33mWARN\033[0m %s\n' "$*"; }   # non-fatal
log()  { printf '\n\033[1;34m== %s ==\033[0m\n' "$*"; }

pkg_of() { # <scala file> -> its declared package, or empty
  sed 's/\r$//' "$1" | grep -m1 -E '^package ' | sed 's/^package  *//'
}

code_of() {
  sed 's/\r$//' "$1" | sed -e 's://.*::' | grep -vE '^[[:space:]]*(\*|/\*)'
}

is_container_suite() { code_of "$1" | grep -qE "$CONTAINER_RE"; }

# ---- 1. container suites must live in the integration package -----------------
log "1. Container-backed suites are all in '$INTEGRATION_PKG'"
found_any=0
while IFS= read -r f; do
  [ -n "$f" ] || continue
  is_container_suite "$f" || continue          # matched only inside a comment -> not a container suite
  found_any=1
  p="$(pkg_of "$f")"
  if [ "$p" = "$INTEGRATION_PKG" ]; then
    ok "$(basename "$f") -> $p"
  else
    bad "$(basename "$f") starts a container but declares 'package $p' (short tier!) -- move it to $INTEGRATION_PKG   [$f]"
  fi
done < <(grep -rlE "$CONTAINER_RE" --include='*.scala' -- */src/test/scala 2>/dev/null | sort)
[ "$found_any" -eq 1 ] || bad "no container-backed suites found at all -- the grep or the layout changed; this gate is not actually checking anything"

# ---- 2. no bare scalatest-maven-plugin ---------------------------------------
log "2. No module leaves <scalatest-maven-plugin> unpinned"
for pom in */pom.xml; do
  grep -q 'scalatest-maven-plugin' "$pom" || continue
  if grep -qE '<wildcardSuites>|<membersOnlySuites>|<suffixes>' "$pom"; then
    ok "$(dirname "$pom") pins its suites"
  else
    bad "$(dirname "$pom") declares scalatest-maven-plugin with no <wildcardSuites> -- it will run EVERY suite in the 'test' phase"
  fi
done

# ---- 2b. every module with test sources actually runs them -------------------
# Invariant 2 only constrains modules that DO declare the plugin. A module with test sources and no
# plugin at all is worse: its suites compile, look like coverage, and never run under Maven.
log "2b. No module owns test sources without a <scalatest-maven-plugin>"
for pom in */pom.xml; do
  mod="$(dirname "$pom")"
  [ -d "$mod/src/test/scala" ] || continue
  [ -n "$(find "$mod/src/test/scala" -name '*.scala' -print -quit 2>/dev/null)" ] || continue
  if grep -q 'scalatest-maven-plugin' "$pom"; then
    ok "$mod runs its test sources"
  else
    bad "$mod has test sources but declares no scalatest-maven-plugin -- its suites never run under Maven"
  fi
done

# ---- 3. integration suites and integration executions agree ------------------
log "3. Modules owning integration suites wire an 'integration-test' execution behind \${skipITs}"
for pom in */pom.xml; do
  mod="$(dirname "$pom")"
  has_suite=0
  if [ -d "$mod/src/test/scala" ]; then
    while IFS= read -r f; do
      [ -n "$f" ] && [ "$(pkg_of "$f")" = "$INTEGRATION_PKG" ] && has_suite=1
    done < <(find "$mod/src/test/scala" -name '*.scala' 2>/dev/null)
  fi
  has_exec=0
  grep -q '<id>integration-test</id>' "$pom" && has_exec=1

  if [ "$has_suite" -eq 1 ] && [ "$has_exec" -eq 0 ]; then
    bad "$mod has $INTEGRATION_PKG suites but no 'integration-test' execution -- they never run (or worse, run at 'test')"
  elif [ "$has_suite" -eq 0 ] && [ "$has_exec" -eq 1 ]; then
    # Non-fatal: a module may keep the standard two-execution template ready for its first
    # integration suite (ignifyr-engine does, since its own ITs were re-homed to the plugin modules).
    warn "$mod wires an 'integration-test' execution but owns no $INTEGRATION_PKG suite yet (template kept ready; harmless)"
  elif [ "$has_suite" -eq 1 ]; then
    if grep -q 'skipTests>\${skipITs}' "$pom"; then
      ok "$mod: integration execution gated by \${skipITs}"
    else
      bad "$mod: integration execution is NOT gated by \${skipITs} -- a local 'mvn install' would start containers"
    fi
  fi
done

# ---- summary -----------------------------------------------------------------
log "Summary"
if [ "$FAILC" -eq 0 ]; then
  printf '  \033[1;32mtest tiers are intact\033[0m\n'
else
  printf '  \033[1;31m%s tier violation(s)\033[0m\n' "$FAILC"
fi
[ "$FAILC" -eq 0 ]
