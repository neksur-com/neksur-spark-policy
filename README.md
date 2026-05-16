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
