/*
 * Neksur Spark Policy — single dispatcher for both Catalyst and SDK paths.
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 *
 * Licensed under the Business Source License 1.1 (BSL 1.1). The Change
 * Date is 2030-05-10; on the Change Date the rights granted in the
 * Change License (Apache License, Version 2.0) become effective.
 * See the LICENSE file at the repository root for the full license text.
 */

package com.neksur.spark.policy.transforms

import com.neksur.spark.policy.{ColumnTransform, Policy, TransformKind}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * `ApplyPolicy` is the SINGLE source of truth for how a `Policy` is
 * materialized into row-level rewrites. Both the Catalyst path
 * (`NeksurPolicyApplier`, dispatch C) and the SDK path
 * (`NeksurDataFrameWriter`, dispatch C) call into this object so that
 * the parity invariant proven by `ExtensionVsSdkParitySpec` (identical
 * Parquet snapshot on both paths) holds by construction — there is no
 * second code path that could drift.
 *
 * Per-column logic lives in `ColumnMask` / `ColumnEncrypt` /
 * `ColumnRedact` / `ColumnTokenize`. This object is just a dispatcher
 * + composer over `Policy.transforms`.
 *
 * The pattern match on `TransformKind` is exhaustive because
 * `TransformKind` is `sealed` — adding a new variant in `PolicyTypes`
 * is a compile error here, not a runtime fall-through. (Pitfall 6
 * mitigation: any new transform kind MUST update this dispatcher.)
 */
object ApplyPolicy {

  /**
   * SDK path — apply each `ColumnTransform` via `DataFrame.withColumn`.
   *
   * Pure function: same `(df, policy)` produces the same DataFrame
   * (modulo Spark's internal plan IDs, which don't affect collected
   * rows or the Parquet snapshot the parity spec compares).
   *
   * Transforms apply in `policy.transforms` order via `foldLeft`, so
   * later transforms see earlier ones — the same column may be
   * transformed twice if listed twice (legal but unusual).
   */
  def applyToDataFrame(df: DataFrame, policy: Policy): DataFrame = {
    policy.transforms.foldLeft(df) { (acc, t) =>
      val transformed: Column = t.kind match {
        case TransformKind.Mask     => ColumnMask.apply(acc, t)
        case TransformKind.Encrypt  => ColumnEncrypt.apply(acc, t)
        case TransformKind.Redact   => ColumnRedact.apply(acc, t)
        case TransformKind.Tokenize => ColumnTokenize.apply(acc, t)
      }
      acc.withColumn(t.column, transformed)
    }
  }

  /**
   * Catalyst path — for use inside `NeksurPolicyApplier` (dispatch C).
   *
   * Phase 2 simplification: this overload's body returns the plan
   * unchanged. The real rewrite is performed by `NeksurPolicyApplier`
   * by materializing the plan into a `DataFrame` via the captured
   * `SparkSession`, calling `applyToDataFrame`, then extracting the
   * resulting `optimizedPlan` from `Dataset.queryExecution`. This
   * keeps a SINGLE code path (`applyToDataFrame`) responsible for the
   * actual per-column rewrites — the Catalyst entry point is just a
   * structural hook so dispatch C can compile against a stable API.
   *
   * The signature MUST exist (dispatch C imports and calls it) but
   * the body intentionally does nothing in Phase 2. Dispatch C wraps
   * the plan→DataFrame→plan round-trip so parity with the SDK path
   * is by-construction (`ExtensionVsSdkParitySpec`).
   */
  def apply(plan: LogicalPlan, policy: Policy): LogicalPlan = {
    // Phase 2: structural placeholder. NeksurPolicyApplier (dispatch C)
    // does the plan → DataFrame → applyToDataFrame → optimizedPlan
    // round-trip using its captured SparkSession; routing rewrites
    // through `applyToDataFrame` keeps both paths byte-identical.
    val _ = policy
    plan
  }
}
