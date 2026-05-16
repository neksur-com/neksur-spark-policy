/*
 * Neksur Spark Policy — regression spec for cross-tenant correlation
 * bypass holes in ColumnTokenize (WR-10 iteration 1 + CR-A1 iteration 2).
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 *
 * Licensed under the Business Source License 1.1 (BSL 1.1).
 * See the LICENSE file at the repository root.
 */

package com.neksur.spark.policy

import com.neksur.spark.policy.transforms.ColumnTokenize
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Locks the fail-closed contract for `ColumnTokenize.apply` against the
 * two distinct mis-configurations that re-introduce cross-tenant
 * correlation:
 *
 *   1. Missing `tenant_salt_id` key (closed by WR-10 in iteration 1 —
 *      `Map.getOrElse(key, default)` fires the default branch).
 *   2. Empty-string `tenant_salt_id` value (closed by CR-A1 in
 *      iteration 2 — `Map.getOrElse` does NOT fire on empty value;
 *      iteration 2 chains `.filter(_.nonEmpty)` to map `Some("")` to
 *      `None` so the same throw fires).
 *
 * Both cases must raise the same `SparkException` carrying the
 * `"tenant_salt_id is required"` substring — operators reading the
 * runtime exception see one consistent failure mode regardless of
 * whether the mis-configuration was a missing key or a blank value.
 *
 * The positive-path sanity test confirms the `.filter(_.nonEmpty)`
 * insertion did not break the happy path that `ApplyPolicySpec` already
 * exercises via the SDK; this file exercises `ColumnTokenize.apply`
 * directly so the unit boundary is narrow.
 */
class ColumnTokenizeSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Stable handle to the SparkSession started lazily by the fixture.
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSessionFixture.spark
  }

  override def afterAll(): Unit = {
    try {
      SparkSessionFixture.stop()
    } finally {
      super.afterAll()
    }
  }

  // ----------------------------------------------------------------- //
  // CR-A1 / WR-10 fail-closed contract
  // ----------------------------------------------------------------- //

  behavior of "ColumnTokenize"

  it should "reject a missing tenant_salt_id with SparkException (WR-10)" in {
    import spark.implicits._

    val df = Seq("123-45-6789").toDF("ssn")
    val transform = ColumnTransform(
      column = "ssn",
      kind = TransformKind.Tokenize,
      params = Map.empty
    )

    val ex = intercept[SparkException] {
      ColumnTokenize.apply(df, transform)
    }
    ex.getMessage should include("tenant_salt_id is required")
  }

  it should "reject an empty-string tenant_salt_id with the same SparkException (CR-A1)" in {
    import spark.implicits._

    val df = Seq("123-45-6789").toDF("ssn")
    val transform = ColumnTransform(
      column = "ssn",
      kind = TransformKind.Tokenize,
      params = Map("tenant_salt_id" -> "")
    )

    val ex = intercept[SparkException] {
      ColumnTokenize.apply(df, transform)
    }
    // Same substring as the missing-key case — one fail-closed contract,
    // two trigger conditions, one error message surface.
    ex.getMessage should include("tenant_salt_id is required")
    // The iteration-2 message explicitly references the empty-string
    // case so an operator reading the runtime exception sees the exact
    // failure mode.
    ex.getMessage should include("missing or empty salt")
  }

  it should "build a tokenized Column when tenant_salt_id is non-empty (positive path)" in {
    import spark.implicits._

    val df = Seq("123-45-6789").toDF("ssn")
    val transform = ColumnTransform(
      column = "ssn",
      kind = TransformKind.Tokenize,
      params = Map("tenant_salt_id" -> "t1")
    )

    noException should be thrownBy ColumnTokenize.apply(df, transform)
  }
}
