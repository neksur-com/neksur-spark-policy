# Neksur Spark Policy

JVM library (Scala + Java) implementing the **L2 pre-write policy enforcement** layer of [Neksur](https://github.com/neksur-com/neksur) — the Open Lakehouse Control Plane for Apache Iceberg.

> **Status: scaffold only.** Initialized 2026-05-16. Full implementation lands as part of Phase 2 (`Cross-Engine Policy Enforcement Core`) — see [`neksur-com/neksur` Phase 2 plans](https://github.com/neksur-com/neksur).

## What this is

This repo ships two complementary frontends to a single canonical pre-write transformation library:

1. **`NeksurEnforcementExtension`** — Spark Catalyst optimizer rule (Approach B). Injected via `SparkSessionExtensions.injectOptimizerRule` to intercept any Spark write to a Neksur-governed Iceberg table — DataFrameWriter, SQL INSERT, Iceberg connector. Rewrites the projection / filter to apply column masks, encryption, and redactions per the table's policy plan, **before** files are written to S3.

2. **`NeksurDataFrameWriter`** — explicit SDK (Approach A). Wrapper class — `df.writeWithNeksur(table).save()` — for teams that prefer explicit opt-in over a silent extension.

Both frontends call into the same `transforms.ApplyPolicy` library. **Identical-snapshot invariant by construction** — `sha256(parquet_bytes)` after column-ordering normalization is asserted equal in `ExtensionVsSdkParitySpec` (CI gate).

## Why a separate repo

This is the first non-Go artifact in the Neksur stack. JVM tooling (sbt, Scala, ScalaTest, Maven publish) belongs in its own build root. Sibling-repo placement matches the multi-repo convention established in Phase 0.5.

## Stack

| Component | Version | Why |
|---|---|---|
| Scala | 2.12.18 | Spark 3.5.x line is binary-compatible with Scala 2.12 |
| Spark | 3.5.4 | Current stable LTS line; Iceberg connector mature |
| Apache Iceberg | iceberg-spark-runtime-3.5_2.12 1.6.1 | Latest 1.6.x patch as of 2026-05-16 |
| Build | sbt | Standard for Scala libraries; `+test` cross-Scala matrix |
| Test | ScalaTest 3.2.x | Idiomatic for Spark library testing |

## Build

sbt 1.9.7 + JDK 17 is the supported build environment. Common commands:

```bash
# Resolve dependencies (offline-friendly after first run; uses Maven Central + ~/.ivy2 cache).
sbt update

# Compile main + test sources.
sbt compile

# Run the test suite (boots a local SparkSession via SparkSessionFixture).
sbt test

# CI cross-Scala matrix (Phase 2 ships only 2.12.18; `+` is future-proofing for 2.13).
sbt +test
```

`Test / fork := true` is set in `build.sbt` — each test class boots its own
JVM so SparkSession singletons stay isolated across tests. Expect ~30s
first-run cost while Spark + Iceberg boot inside the forked JVM.

GitHub Actions CI runs `sbt +test` on every push and PR — see
[`.github/workflows/sbt-test.yml`](.github/workflows/sbt-test.yml).

### Spark + Iceberg Compatibility Matrix

> **⚠ Warning — version drift bites hard.** Spark + Iceberg version pairs
> are not freely substitutable. The Iceberg runtime jar is
> Spark-version-specific (`iceberg-spark-runtime-3.5_2.12` is bound to
> Spark 3.5 + Scala 2.12); pairing Iceberg 1.5.x with Spark 3.5 (or 1.6.x
> with Spark 3.4) silently disables plan-class features (e.g.,
> `V2WriteCommand` shape changes) and produces hard-to-trace failures
> at write time. **Always pin the exact pair below.**

| Component | Pinned Version | Notes |
|---|---|---|
| Scala     | 2.12.18 | Spark 3.5 canonical (only 2.12 cell in Phase 2; 2.13 deferred) |
| Spark     | 3.5.4   | Current 3.5.x LTS patch (Apr 2026 release) |
| Iceberg   | 1.6.1   | `iceberg-spark-runtime-3.5_2.12` matched to Spark 3.5 |
| JDK       | 17      | Spark 3.5 supports JDK 8/11/17; 17 is the modern LTS pick |
| sbt       | 1.9.7   | Pinned in `project/build.properties` |

Future cells (deferred): Spark 3.5 + Iceberg 1.5.0 (back-compat); Scala 2.13 (Spark 4.x prep). See Phase 2 RESEARCH §Pitfall 5 for details.

## License

**Business Source License 1.1** with a four-year **Change Date** to the **Apache License 2.0** — same license as [`neksur-com/neksur`](https://github.com/neksur-com/neksur). See [`LICENSE`](LICENSE) for the full text and [`LICENSE.md`](LICENSE.md) for a plain-English summary.

## Contributing

Contribution policy + DCO sign-off conventions inherited from `neksur-com/neksur`. A standalone `CONTRIBUTING.md` will land alongside the first real source code in Phase 2.

## Repository Map

This is the **Neksur Spark Policy** repository. Related repositories under the `neksur-com` organization:

| Repository | Visibility | License | Purpose |
|---|---|---|---|
| [`neksur-com/neksur`](https://github.com/neksur-com/neksur) | public | BSL 1.1 → Apache 2.0 (2030-05-10) | Neksur Core source code (Go monorepo) |
| [`neksur-com/neksur-premium`](https://github.com/neksur-com/neksur-premium) | private | Neksur Commercial License | Commercial Premium components |
| [`neksur-com/docs`](https://github.com/neksur-com/docs) | public | Apache 2.0 | Public documentation |
| `neksur-com/neksur-spark-policy` (this repo) | public | BSL 1.1 → Apache 2.0 (2030-05-16) | L2 Spark policy enforcement library |

## Contact

- General: `hello@neksur.com`
- Issues: open a GitHub issue on this repo
- Architecture / roadmap questions: open an issue on [`neksur-com/neksur`](https://github.com/neksur-com/neksur)
