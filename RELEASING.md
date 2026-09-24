# Releasing Ignifyr Community

**A release is a git tag, the library modules deployed to SRDC Nexus, the standalone fat jar, and the
Docker image built from it.** The Nexus deployment exists for one consumer: the Enterprise Edition,
which lives in its own private repository and builds on these artifacts (its parent pom *is* this
repository's root pom). Maven Central is deliberately not a target. `ignifyr-cli` is not deployed —
it is a distribution, not a library. Between releases, CI deploys `main`'s `-SNAPSHOT` to Nexus on
every push (`.github/workflows/maven.yml`, `deploy-snapshot`), so the enterprise edition can track
unreleased community work. The `sources` profile still produces source and javadoc jars.

The fat jar shapes most of this document: because it *shades* every dependency, it redistributes
them, and Ignifyr inherits each one's obligations. The interesting questions are about what is
**inside** the jar, not what the poms declare. That is what
[`test-flow/check-release-ready.sh`](test-flow/check-release-ready.sh) checks.

| Deliverable | Built from |
|---|---|
| `io.ignifyr:*` library modules + the `ignifyr_2.13` parent pom on SRDC Nexus | `mvn deploy` |
| `ignifyr-cli/target/ignifyr-engine-standalone.jar` | `ignifyr-cli` |
| `srdc/ignifyr-engine` | `docker/engine/build.sh` |

The enterprise server jar and image are released from the enterprise repository, after (and against)
a community release.

## Versioning

The version is `<revision>` in the root [pom.xml](pom.xml); every module inherits it through
flatten-maven-plugin, which bakes it into a literal in the installed and deployed poms
(`resolveCiFriendliesOnly` — the deployed root pom must keep its build configuration, because the
enterprise edition inherits it). Between releases it is `<next>-SNAPSHOT`. A release sets it to the bare
version, tags `v<version>`, and then opens the next development version.

Ignifyr's own `${revision}` must be the **only** `-SNAPSHOT` in the build. An upstream snapshot
makes the jar unreproducible from the tag, which is the one property a tag is supposed to carry.

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

The long tier needs Docker.

## 2. Cut the version

Set `<revision>` in the root pom to the release version, commit it on its own, and rebuild.

## 3. Verify the release artifacts

```bash
bash test-flow/check-release-ready.sh --release
```

In `--release` mode every check is a hard failure. It rebuilds and installs the fat jar and
its upstream modules — the dependency listings resolve the sibling modules from the local
repository — then asserts:

1. **Nothing is a `-SNAPSHOT`** — neither `${revision}` nor any resolved dependency.
2. **Attribution survives shading** — the jar's `META-INF/NOTICE` aggregates the ~75 bundled
   NOTICEs rather than whichever single copy shade saw last, and `META-INF/LICENSE` is the
   repository's own. Section 4(d) of the Apache License requires carrying these forward.
3. **No copyleft on the community distribution** — Repofyr, the onFHIR server continuation, is
   GPL-3.0 and sits one dependency edge from code Ignifyr already uses. Multi-licensed dependencies
   pass when they offer a permissive alternative; only copyleft-*only* artifacts fail.
4. **Release hygiene** — clean working tree, tag not already taken.

Run it without `--release` as a per-commit guard; the version checks drop to warnings then.

## 4. Tag and publish — maintainer only

> **Stop here unless you are the maintainer cutting this release, and do it yourself.**
> Everything above is local and reversible. Everything below is not: a pushed tag, a pushed
> image and a deployed release artifact are public (Nexus refuses to redeploy a release version). Automation and agents run sections 1–3 and stop; they do not push, tag, or
> publish, and they do not disable a failing gate to get to green.

Tag, then build and tag the images with the version — not only `latest`, which is all the
`build.sh` scripts do today:

```bash
git tag -a v<version> -m "Ignifyr <version>" && git push origin v<version>
```

```bash
bash docker/engine/build.sh && docker tag srdc/ignifyr-engine:latest srdc/ignifyr-engine:<version>
```

Deploy the library modules to SRDC Nexus (credentials from the `srdc-maven-releases` server entry in
your `settings.xml`):

```bash
mvn -B -DskipTests deploy
```

Attach the standalone jar to the GitHub release for the tag.

## 5. Post-release

Set `<revision>` to the next `-SNAPSHOT` and commit. Then, in the enterprise repository, point the
parent version at the new community release before cutting the matching enterprise release. Update [CLAUDE.md](CLAUDE.md) if the release
changed anything an agent relies on.

## Known limitations

- **Two NOTICEs cannot be aggregated.** `scala-library` and `scala-reflect` put `NOTICE` at the jar
  root rather than under `META-INF/`, where no shade transformer can reach it. 73 of the 75 bundled
  NOTICEs are merged. Closing the gap means hand-placing a `META-INF/NOTICE` resource in each
  distribution module, which then goes stale silently — judged not worth it.
- **Dependencies that declare no license** are reported, never failed — roughly half inherit it from
  a parent pom that is not resolved locally. Guessing would make the gate untrustworthy.
