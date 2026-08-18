#!/usr/bin/env bash
#
# Release-readiness gate.
#
# Ignifyr publishes no Maven artifacts -- nothing depends on it as a library. A release is a tag,
# the two fat jars, and the Docker images built from them. That makes the interesting question
# "what is inside the jars" rather than "what does the pom declare", because a shaded jar
# *redistributes* every dependency and inherits its obligations. Five invariants:
#
#   1. Nothing resolves to a -SNAPSHOT, so the jars are rebuildable from the tag.
#   2. Both jars carry aggregated third-party NOTICEs and Ignifyr's own LICENSE (Apache-2.0 4(d)).
#   3. No copyleft artifact reaches the Apache-2.0 community distribution. Repofyr -- the onFHIR
#      server continuation -- is GPL-3.0 and sits one dependency edge from code we already use.
#   4. ignifyr-terminology-tools is in neither distribution: it embeds hard-coded dev Postgres
#      credentials, which is exactly why it ships nowhere.
#   5. The working tree is clean and the tag is free, so the tag names what was verified.
#
# Two modes. Bare, it is a per-commit guard: version checks WARN, because a development ${revision}
# is legitimately a snapshot. With --release they are hard failures.
#
#   test-flow/check-release-ready.sh              # dev guard (mainly invariants 2-4)
#   test-flow/check-release-ready.sh --release    # release gate; everything must pass
#
# Builds and installs the two distributions and their upstream modules (tests skipped) before
# checking, so the dependency listings resolve on a clean checkout. No Docker.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

CLI_JAR="$REPO_ROOT/ignifyr-cli/target/ignifyr-engine-standalone.jar"
SRV_JAR="$REPO_ROOT/ignifyr-server/target/ignifyr-server-standalone.jar"

RELEASE_MODE=0
[ "${1:-}" = "--release" ] && RELEASE_MODE=1

PASS=0; FAILC=0
ok()   { printf '  \033[1;32mPASS\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '  \033[1;31mFAIL\033[0m %s\n' "$*"; FAILC=$((FAILC+1)); }
warn() { printf '  \033[1;33mWARN\033[0m %s\n' "$*"; }
log()  { printf '\n\033[1;34m== %s ==\033[0m\n' "$*"; }
# Fails only when releasing; a development tree is allowed to be mid-flight.
soft() { if [ "$RELEASE_MODE" -eq 1 ]; then bad "$*"; else warn "$* (dev mode)"; fi; }

# The community fat jar is a zip64 archive Info-ZIP cannot always enumerate; Python's zipfile can.
# Same fallback shape as check-editions.sh. Note the byte-level stdout: Python's text mode would
# translate every LF to CRLF on Windows and inflate the size checks below by one byte per line.
PY="$(command -v python 2>/dev/null || command -v python3 2>/dev/null || true)"
PY_LIST='import sys,zipfile
for n in zipfile.ZipFile(sys.argv[1]).namelist(): print(n)'
PY_READ='import sys,zipfile
try: sys.stdout.buffer.write(zipfile.ZipFile(sys.argv[1]).read(sys.argv[2]))
except KeyError: pass'

jar_list() { # <jar> -> entry names, one per line
  if [ -n "$PY" ]; then "$PY" -c "$PY_LIST" "$1" 2>/dev/null
  else unzip -l "$1" 2>/dev/null | awk '{print $NF}'; fi
}
jar_read() { # <jar> <entry> -> raw bytes on stdout, empty if absent
  if [ -n "$PY" ]; then "$PY" -c "$PY_READ" "$1" "$2" 2>/dev/null
  else unzip -p "$1" "$2" 2>/dev/null; fi
}

log "(Re)building the community + server fat jars"
# `install`, not `package`: the dependency:list calls below resolve ignifyr-cli's sibling
# modules from the local repository, and CI only ever runs `verify`/`package`, which do not
# put them there. Without this the listings come back empty on a clean runner.
mvn -q -DskipTests -pl ignifyr-cli,ignifyr-server -am install || { echo "build failed" >&2; exit 1; }
[ -f "$CLI_JAR" ] && [ -f "$SRV_JAR" ] || { echo "jars missing after build" >&2; exit 1; }

# ---- 1. no -SNAPSHOT anywhere ------------------------------------------------
log "1. Nothing resolves to a -SNAPSHOT"
REVISION="$(grep -m1 -oE '<revision>[^<]+</revision>' pom.xml | sed 's/<[^>]*>//g')"
case "$REVISION" in
  *-SNAPSHOT) soft "\${revision} is $REVISION -- set a release version before tagging" ;;
  *)          ok "\${revision} is $REVISION" ;;
esac

DEPS="$(mktemp)"; CLI_DEPS="$(mktemp)"
trap 'rm -f "$DEPS" "$CLI_DEPS"' EXIT
mvn -q dependency:list -pl ignifyr-cli,ignifyr-server -DincludeScope=runtime \
    -DoutputFile="$DEPS" -DappendOutput=true >/dev/null 2>&1
# An empty listing would make every dependency check below pass vacuously.
if ! grep -qE ':[^:]+:[^:]+:' "$DEPS" 2>/dev/null; then
  bad "dependency:list produced no output -- the dependency checks cannot run"
fi
SNAPS="$(sed 's/^ *//' "$DEPS" 2>/dev/null | grep -E ':[^:]*-SNAPSHOT' | sort -u || true)"
if [ -z "$SNAPS" ]; then
  ok "no -SNAPSHOT on either distribution runtime classpath"
else
  # Ignifyr's own modules carry ${revision}; they stop being snapshots once the check above passes.
  OWN="$(echo "$SNAPS" | grep -c '^io\.ignifyr:' || true)"
  THIRD="$(echo "$SNAPS" | grep -v '^io\.ignifyr:' || true)"
  if [ -n "$THIRD" ]; then
    bad "third-party -SNAPSHOT dependencies would make the jars unreproducible:"
    echo "$THIRD" | sed 's/^/        /'
  else
    soft "$OWN Ignifyr module(s) still at a -SNAPSHOT \${revision}"
  fi
fi

# ---- 2. attribution inside the shaded jars -----------------------------------
log "2. Both jars carry aggregated NOTICEs and Ignifyr's own LICENSE"
REPO_LICENSE_BYTES="$(wc -c < LICENSE | tr -d ' ')"
# ~75 bundled jars ship a NOTICE; one un-merged copy is a few hundred bytes, the merged file tens of
# kB. The floor catches an attribution transformer silently dropping out of the shade config.
NOTICE_MIN_BYTES=10000
for pair in "community:$CLI_JAR" "server:$SRV_JAR"; do
  name="${pair%%:*}"; jar="${pair#*:}"
  n="$(jar_read "$jar" META-INF/NOTICE | wc -c | tr -d ' ')"
  if [ "${n:-0}" -ge "$NOTICE_MIN_BYTES" ]; then
    ok "$name jar: META-INF/NOTICE is aggregated ($n bytes)"
  else
    bad "$name jar: META-INF/NOTICE is only ${n:-0} bytes -- third-party attribution is being discarded"
  fi
  l="$(jar_read "$jar" META-INF/LICENSE | wc -c | tr -d ' ')"
  if [ "${l:-0}" = "$REPO_LICENSE_BYTES" ]; then
    ok "$name jar: META-INF/LICENSE is the repository's own ($l bytes)"
  else
    bad "$name jar: META-INF/LICENSE is ${l:-0} bytes, expected the repo LICENSE ($REPO_LICENSE_BYTES)"
  fi
done

# ---- 3. no copyleft in the Apache-2.0 community distribution -----------------
log "3. No copyleft artifact on the community distribution"
mvn -q dependency:list -pl ignifyr-cli -DincludeScope=runtime -DoutputFile="$CLI_DEPS" >/dev/null 2>&1
if ! grep -qE ':[^:]+:[^:]+:' "$CLI_DEPS" 2>/dev/null; then
  bad "dependency:list produced no community listing -- the licence checks cannot run"
fi

# Coordinate denylist first: the concrete, known risk. Repofyr is the GPL-3.0 onFHIR server
# continuation; onfhir-server-r4/r5 were its artifacts before the 4.0.0 split.
HITS="$(sed 's/^ *//' "$CLI_DEPS" 2>/dev/null | grep -E '^io\.repofyr:|:onfhir-server-r[45]' || true)"
if [ -z "$HITS" ]; then
  ok "no denylisted copyleft coordinate on the community tree"
else
  bad "copyleft coordinate on the Apache-2.0 community distribution:"
  echo "$HITS" | sed 's/^/        /'
fi

# Then whatever the dependencies' own poms declare. Multi-licensing is the norm here and a naive
# scan cries wolf: jakarta.* and jersey offer "EPL 2.0 OR GPL2 w/ CPE", rocksdbjni "Apache 2.0 OR
# GPLv2", javassist "MPL/LGPL/Apache". A dependency is a problem only when *every* license it
# offers is strong copyleft -- GPL/AGPL without a classpath exception and not the Lesser variant.
# Poms that inherit their license from an unresolved parent declare none; those are reported, not
# failed, since guessing would make the gate untrustworthy.
PY_LICENSE='import re, sys, os
dep_re = re.compile(r"^\s*([\w.\-]+):([\w.\-]+):[\w.\-]+:([\w.\-]+)")
name_re = re.compile(r"<licenses>(.*?)</licenses>", re.S)
each_re = re.compile(r"<name>(.*?)</name>", re.S)
PERMISSIVE = ("apache", "mit", "bsd", "epl", "eclipse public", "eclipse distribution", "edl",
              "cddl", "mpl", "mozilla public", "public domain", "unlicense", "isc", "zlib",
              "w3c", "bouncy castle", "go license", "creative commons")
def excepted(n):
    # A classpath exception defuses the copyleft for a bundled application. Spelled out, or the
    # "GPL2 w/ CPE" shorthand the jakarta.* and glassfish poms use.
    return ("classpath" in n and "exception" in n) or bool(re.search(r"\bcpe\b", n))
def strong_copyleft(n):
    n = n.lower()
    if excepted(n): return False
    if "lesser" in n or re.search(r"\blgpl\b", n): return False
    # Both the abbreviations and the spelled-out names, which "gnu general public" alone misses for
    # Affero ("GNU Affero General Public License" has a word wedged in the middle).
    return bool(re.search(r"\ba?gpl[-\s]?\d?", n) or "general public license" in n)
def permissive(n):
    n = n.lower()
    if excepted(n): return True
    if "lesser" in n or re.search(r"\blgpl\b", n): return True
    return any(p in n for p in PERMISSIVE)
m2 = os.path.join(os.path.expanduser("~"), ".m2", "repository")
checked = unknown = 0; bad_ones = []
for line in open(sys.argv[1], encoding="utf-8", errors="replace"):
    m = dep_re.match(line)
    if not m: continue
    g, a, v = m.group(1), m.group(2), m.group(3)
    pom = os.path.join(m2, *g.split("."), a, v, a + "-" + v + ".pom")
    if not os.path.isfile(pom): continue
    checked += 1
    try: text = open(pom, encoding="utf-8", errors="replace").read()
    except OSError: continue
    blk = name_re.search(text)
    names = [n.strip() for n in each_re.findall(blk.group(1))] if blk else []
    if not names: unknown += 1; continue
    if any(permissive(n) for n in names): continue
    if any(strong_copyleft(n) for n in names):
        bad_ones.append(g + ":" + a + ":" + v + "  [" + " | ".join(names) + "]")
for b in bad_ones: print("FAIL " + b)
print("STAT %d %d %d" % (checked, unknown, len(bad_ones)))'

if [ -n "$PY" ]; then
  LICOUT="$("$PY" -c "$PY_LICENSE" "$CLI_DEPS" 2>/dev/null)"
  while IFS= read -r l; do
    case "$l" in
      FAIL*) bad "community dependency offers only copyleft terms: ${l#FAIL }" ;;
      STAT*) set -- $l
             ok "no community dependency is copyleft-only ($2 poms read, $3 declare no license)" ;;
    esac
  done <<EOF
$LICOUT
EOF
else
  warn "python not found; skipping the declared-license scan (coordinate denylist still ran)"
fi

# ---- 4. the credential-carrying module ships nowhere -------------------------
log "4. ignifyr-terminology-tools is in neither distribution"
for pair in "community:$CLI_JAR" "server:$SRV_JAR"; do
  name="${pair%%:*}"; jar="${pair#*:}"
  if jar_list "$jar" | grep -q '^io/ignifyr/terminology/'; then
    bad "$name jar bundles ignifyr-terminology-tools, which embeds hard-coded dev Postgres credentials"
  else
    ok "$name jar excludes ignifyr-terminology-tools"
  fi
done

# ---- 5. release hygiene ------------------------------------------------------
log "5. Release hygiene"
if [ -z "$(git status --porcelain 2>/dev/null)" ]; then
  ok "working tree is clean"
else
  soft "working tree has uncommitted changes -- the tag would not name what was built"
fi
if [ -n "$REVISION" ] && git rev-parse -q --verify "refs/tags/v$REVISION" >/dev/null 2>&1; then
  soft "tag v$REVISION already exists"
else
  ok "tag v$REVISION is free"
fi

# ---- summary -----------------------------------------------------------------
log "Summary"
printf '  passed: %s   failed: %s   mode: %s\n' "$PASS" "$FAILC" \
  "$([ "$RELEASE_MODE" -eq 1 ] && echo release || echo dev)"
[ "$FAILC" -eq 0 ]
