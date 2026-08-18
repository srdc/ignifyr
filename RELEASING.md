# Releasing Ignifyr

**A release is a git tag, two fat jars, and the Docker images built from them.** Ignifyr publishes
no Maven artifacts — nothing consumes it as a library, so there is no `<distributionManagement>` and
`mvn deploy` has no target. That is deliberate, not an oversight.

The consequence shapes this whole document: because the jars *shade* every dependency, they
redistribute them, and Ignifyr inherits each one's obligations. The interesting questions are about
what is **inside** the jars, not what the poms declare. That is what
[`test-flow/check-release-ready.sh`](test-flow/check-release-ready.sh) checks.

| Deliverable | Built from | Edition |
|---|---|---|
| `ignifyr-cli/target/ignifyr-engine-standalone.jar` | `ignifyr-cli` | Community (Apache-2.0) |
| `ignifyr-server/target/ignifyr-server-standalone.jar` | `ignifyr-server` | Enterprise |
| `srdc/ignifyr-engine` | `docker/engine/build.sh` | Community |
| `srdc/ignifyr-server` | `docker/server/build.sh` | Enterprise |

## Versioning

The version is `<revision>` in the root [pom.xml](pom.xml); every module inherits it through
flatten-maven-plugin. Between releases it is `<next>-SNAPSHOT`. A release sets it to the bare
version, tags `v<version>`, and then opens the next development version.

Ignifyr's own `${revision}` must be the **only** `-SNAPSHOT` in the build. An upstream snapshot
makes the jars unreproducible from the tag, which is the one property a tag is supposed to carry.

## 1. Pre-flight

Build and test on **JDK 11** — that is what CI uses. Check `mvn --version` first.

```bash
mvn scalafmt:format && mvn -B -DskipTests install
```

Then the gates that already exist, in increasing cost. All must be green. Invoke them through
`bash` — the repository does not carry the executable bit on its scripts, and CI does the same:

```bash
bash test-flow/check-test-tiers.sh && bash test-flow/check-editions.sh && bash test-flow/check-enforcer-gate.sh
```

```bash
mvn -B test
```

```bash
mvn -B verify -DskipITs=false
```

The long tier needs Docker. If the streaming suites fail with `CONCURRENT_STREAM_LOG_UPDATE`, clear
stale checkpoint state first:

```bash
rm -rf ignifyr-server/test-context-conf ignifyr-server/logs ignifyr-runtime-streaming/checkpoint ignifyr-runtime-streaming/logs
```

## 2. Cut the version

Set `<revision>` in the root pom to the release version, commit it on its own, and rebuild.

## 3. Verify the release artifacts

```bash
bash test-flow/check-release-ready.sh --release
```

In `--release` mode every check is a hard failure. It rebuilds both fat jars and asserts:

1. **Nothing is a `-SNAPSHOT`** — neither `${revision}` nor any resolved dependency.
2. **Attribution survives shading** — each jar's `META-INF/NOTICE` aggregates the ~75 bundled
   NOTICEs rather than whichever single copy shade saw last, and `META-INF/LICENSE` is the
   repository's own. Section 4(d) of the Apache License requires carrying these forward.
3. **No copyleft on the community distribution** — Repofyr, the onFHIR server continuation, is
   GPL-3.0 and sits one dependency edge from code Ignifyr already uses. Multi-licensed dependencies
   pass when they offer a permissive alternative; only copyleft-*only* artifacts fail.
4. **`ignifyr-terminology-tools` is in neither jar** — it embeds hard-coded dev Postgres
   credentials, which is why it ships nowhere.
5. **Release hygiene** — clean working tree, tag not already taken.

Run it without `--release` as a per-commit guard; the version checks drop to warnings then.

## 4. Tag and publish — maintainer only

> **Stop here unless you are the maintainer cutting this release, and do it yourself.**
> Everything above is local and reversible. Everything below is not: a pushed tag and a pushed
> image are public. Automation and agents run sections 1–3 and stop; they do not push, tag, or
> publish, and they do not disable a failing gate to get to green.

Tag, then build and tag the images with the version — not only `latest`, which is all the
`build.sh` scripts do today:

```bash
git tag -a v<version> -m "Ignifyr <version>" && git push origin v<version>
```

```bash
bash docker/engine/build.sh && docker tag srdc/ignifyr-engine:latest srdc/ignifyr-engine:<version>
```

```bash
bash docker/server/build.sh && docker tag srdc/ignifyr-server:latest srdc/ignifyr-server:<version>
```

Attach both standalone jars to the GitHub release for the tag.

## 5. Post-release

Set `<revision>` to the next `-SNAPSHOT` and commit. Update [CLAUDE.md](CLAUDE.md) if the release
changed anything an agent relies on.

## Known limitations

- **Two NOTICEs cannot be aggregated.** `scala-library` and `scala-reflect` put `NOTICE` at the jar
  root rather than under `META-INF/`, where no shade transformer can reach it. 73 of the 75 bundled
  NOTICEs are merged. Closing the gap means hand-placing a `META-INF/NOTICE` resource in each
  distribution module, which then goes stale silently — judged not worth it.
- **Dependencies that declare no license** are reported, never failed — roughly half inherit it from
  a parent pom that is not resolved locally. Guessing would make the gate untrustworthy.
- **The `release` profile is dead code.** It wires `nexus-staging-maven-plugin` against
  `oss.sonatype.org`, the retired OSSRH service, alongside source/javadoc/GPG artifacts that only a
  Maven consumer would want. Nothing invokes it. If Ignifyr ever does publish Maven artifacts, that
  profile needs rebuilding against the Central Portal — not reviving as-is.
