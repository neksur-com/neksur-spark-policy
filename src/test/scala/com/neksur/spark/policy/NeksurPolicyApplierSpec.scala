/*
 * Neksur Spark Policy — NeksurPolicyApplier specs.
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 * Licensed under the Business Source License 1.1 (BSL 1.1).
 * See the LICENSE file at the repository root.
 */

package com.neksur.spark.policy

import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * `NeksurPolicyApplier` Catalyst-rule coverage.
 *
 * Three structural tests — full plan-rewrite paths are exercised by
 * `ExtensionVsSdkParitySpec` (which proves the by-construction parity
 * with the SDK path) and by Plan 02-08 E2E (which writes to a real
 * local Iceberg catalog). Here we lock in:
 *
 *   1. The rule constructs cleanly from a live `SparkSession`.
 *   2. Pitfall 2 mitigation: plans already tagged with
 *      `neksur.transformed` are left strictly unchanged — same
 *      reference, no rewrite (would otherwise cause double-encryption
 *      on Catalyst's fixed-point batch).
 *   3. Plans for non-governed tables are no-ops — leaving
 *      `spark.neksur.governed_tables` unset MUST result in a
 *      pass-through for any plan, even ones that would otherwise
 *      match `isV2WriteCommand`.
 *
 * SparkSession lifetime: this spec uses `SparkSessionFixture.spark`
 * but does NOT call `SparkSessionFixture.stop()` — the singleton is
 * shared across specs in the same forked JVM (per Plan 02-02 fixture
 * docs, `Test / fork := true` gives each spec class its own JVM, so
 * JVM exit releases the SparkSession deterministically).
 */
class NeksurPolicyApplierSpec extends AnyFlatSpec with Matchers {

  // Spark internally calls TreeNodeTag.apply; we reconstruct the same
  // key the production code uses so we can pre-mark plans the same way
  // the rule itself would.
  private val transformedTag = TreeNodeTag[Boolean]("neksur.transformed")

  behavior of "NeksurPolicyApplier"

  it should "construct from SparkSession" in {
    // Smoke: the rule's constructor must not touch PolicyClient or
    // SparkConf eagerly (the `policyClient` field is `private lazy
    // val` precisely so registration order with Spark's extension
    // factory stays safe).
    val applier = new NeksurPolicyApplier(SparkSessionFixture.spark)
    applier should not be null
  }

  it should "leave plans already marked transformed unchanged" in {
    // Pitfall 2: Catalyst's optimizer runs rules to a fixed point. The
    // TreeNodeTag marker is what prevents the rule from re-applying
    // to a plan it just rewrote. We pre-mark a leaf plan and assert
    // the rule's `apply` returns the same reference back unchanged.
    val plan = LocalRelation()
    plan.setTagValue(transformedTag, true)

    val applier = new NeksurPolicyApplier(SparkSessionFixture.spark)
    val out = applier.apply(plan)

    // Same reference (no rewrite happened) AND the tag is still set.
    out should be theSameInstanceAs plan
    out.getTagValue(transformedTag) shouldBe Some(true)
  }

  it should "leave plans for non-governed tables unchanged" in {
    // Without `spark.neksur.governed_tables` set, NO plan should be
    // considered governed — `isNeksurGoverned` short-circuits to
    // `false` on an empty config. We pass a plain `LocalRelation`
    // (not a V2 write command) which also fails `isV2WriteCommand`,
    // so the rule's `transform` block must not match, and the same
    // reference must come back unchanged.
    val conf = SparkSessionFixture.spark.sparkContext.getConf
    conf.remove("spark.neksur.governed_tables")

    val plan = LocalRelation()
    val applier = new NeksurPolicyApplier(SparkSessionFixture.spark)
    val out = applier.apply(plan)

    out should be theSameInstanceAs plan
  }
}
