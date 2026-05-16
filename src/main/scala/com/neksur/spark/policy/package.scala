/*
 * Neksur Spark Policy — L2 pre-write policy enforcement library.
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 *
 * Licensed under the Business Source License 1.1 (BSL 1.1). The Change
 * Date is 2030-05-10; on the Change Date the rights granted in the
 * Change License (Apache License, Version 2.0) become effective.
 * See the LICENSE file at the repository root for the full license text.
 */

package com.neksur.spark

/**
 * '''com.neksur.spark.policy''' — canonical Spark transformation library
 * for Neksur's L2 pre-write policy enforcement.
 *
 * == Phase 2 contract ==
 *
 * This package object is the public namespace anchor for the JVM library
 * shipped from `neksur-com/neksur-spark-policy`. Phase 2 Plan 02-02
 * (this commit) reserves the package and ships only the build scaffold;
 * Plan 02-06 lands the real source set:
 *
 *   - `NeksurEnforcementExtension` — Spark Catalyst optimizer rule
 *     (Approach B, D-2.06). Injected via
 *     `SparkSessionExtensions.injectOptimizerRule` to intercept
 *     `V2WriteCommand` plans and rewrite the projection / filter for
 *     masking, encryption, and redaction before files land on S3.
 *
 *   - `NeksurDataFrameWriter` — explicit SDK (Approach A, D-2.06).
 *     Thin wrapper `df.writeWithNeksur(table).save()` that calls the
 *     same `transforms.ApplyPolicy` function. Identical-snapshot
 *     invariant is by construction (Phase 2 §6).
 *
 *   - `transforms.ApplyPolicy` — single canonical
 *     `applyPolicy(plan, policy)` function both frontends call.
 *
 *   - `KmsKeyProvider` — wraps AWS KMS `GenerateDataKey` for
 *     per-column DEK derivation (D-2.07); per-batch in-process cache.
 *
 * == Versioning ==
 *
 * `Version` is the read-only library version string. Tests use it to
 * confirm the build links correctly without instantiating any of the
 * not-yet-implemented classes above.
 */
package object policy {

  /**
   * Library version string. Bumped manually for each release; matches
   * `version` in `build.sbt`. Plan 02-08 wires automated Maven publish
   * which will assert build.sbt and this constant stay in lockstep.
   */
  val Version: String = "0.1.0-SNAPSHOT"

}
