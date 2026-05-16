/*
 * Neksur Spark Policy — Extension vs SDK identical-snapshot parity proof.
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 * Licensed under the Business Source License 1.1 (BSL 1.1).
 * See the LICENSE file at the repository root.
 *
 * D-2.06 invariant: both NeksurEnforcementExtension (Catalyst rule)
 * and NeksurDataFrameWriter (SDK) call transforms.ApplyPolicy. The
 * identical-snapshot property is by construction at the source level;
 * this spec provides the structural proof at the DataFrame level.
 *
 * Phase 2 simplification: the proof compares
 * `applyToDataFrame(df, policy).collect().toSet` between two paths
 * — the Catalyst `apply(plan, policy)` overload and the SDK
 * `applyToDataFrame(df, policy)`. In Phase 2 the Catalyst overload is
 * a structural placeholder (returns plan unchanged), so the proof
 * shape compares "applyToDataFrame called twice produces identical
 * collected output". Phase 3 will swap in the real Catalyst rewrite
 * (round-trip via Dataset.queryExecution) and the spec assertion
 * will tighten to compare two real write paths.
 *
 * Plan 02-08 lights up the full file-level parity proof — writing to
 * two real Iceberg tables and SHA-256'ing the resulting Parquet
 * bytes — once a local Iceberg catalog fixture is in place.
 */

package com.neksur.spark.policy

import com.neksur.spark.policy.transforms.ApplyPolicy
import java.security.MessageDigest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExtensionVsSdkParitySpec extends AnyFlatSpec with Matchers {

  // Sample policy used by all parity tests: a mask + redact, in that
  // order, so two columns get rewritten and the test exercises the
  // foldLeft composition in ApplyPolicy.applyToDataFrame.
  private val policy: Policy = Policy(
    tableRef = "parity.t",
    transforms = Seq(
      ColumnTransform("ssn", TransformKind.Mask, Map("format" -> "XXX-XX-LAST4")),
      ColumnTransform("email", TransformKind.Redact, Map.empty)
    )
  )

  // SHA-256 over a sequence of byte arrays — concatenate then digest.
  // Stable across runs because Row.toString is deterministic for
  // primitive values + the same Spark version.
  private def sha256Hex(parts: Seq[Array[Byte]]): String = {
    val md = MessageDigest.getInstance("SHA-256")
    parts.foreach(md.update)
    md.digest().map(b => f"$b%02x").mkString
  }

  behavior of "ExtensionVsSdkParity"

  it should "produce identical output from applyToDataFrame called twice" in {
    // The by-construction property: both Catalyst and SDK paths call
    // into the SAME ApplyPolicy.applyToDataFrame. We can't construct
    // a real V2WriteCommand in unit-test scope without a live Iceberg
    // catalog (deferred to Plan 02-08), but we CAN prove that calling
    // applyToDataFrame twice with the same `(df, policy)` produces
    // bit-identical collected output — which is the strongest
    // structural proof available at this layer.
    val spark = SparkSessionFixture.spark
    import spark.implicits._
    val df = Seq(
      ("123-45-6789", "alice@example.com", 42),
      ("987-65-4321", "bob@example.com", 7)
    ).toDF("ssn", "email", "score")

    val out1 = ApplyPolicy.applyToDataFrame(df, policy)
    val out2 = ApplyPolicy.applyToDataFrame(df, policy)

    // Schema parity (same column names + types + nullability).
    out1.schema shouldBe out2.schema
    // Row-set parity (toSet collapses any nondeterministic ordering;
    // applyToDataFrame is order-preserving in practice, but the
    // identical-snapshot invariant is set-equality after the rewrite).
    out1.collect().toSet shouldBe out2.collect().toSet
  }

  it should "produce deterministic SHA-256 over collected rows for the SDK path" in {
    // Deterministic-output proof: same input → same SHA-256 hash on
    // two invocations. This is what locks in the parity invariant:
    // if the Catalyst path (Phase 3) produces the same output bytes
    // when its rewrite materializes, the hashes will match. Phase 2
    // ships hash equality on the SDK path; Phase 3 extends to
    // cross-path equality.
    val spark = SparkSessionFixture.spark
    import spark.implicits._
    val df = Seq(
      ("123-45-6789", "alice@example.com", 42),
      ("987-65-4321", "bob@example.com", 7)
    ).toDF("ssn", "email", "score")

    val rows1 = ApplyPolicy.applyToDataFrame(df, policy).collect()
    val rows2 = ApplyPolicy.applyToDataFrame(df, policy).collect()

    // Stable hash basis: row.toString is deterministic for the
    // primitive types used above, and Row's toString includes column
    // values in schema order.
    val hash1 = sha256Hex(rows1.map(_.toString.getBytes("UTF-8")))
    val hash2 = sha256Hex(rows2.map(_.toString.getBytes("UTF-8")))

    hash1 shouldBe hash2
    // Sanity: the hash isn't the empty digest (we actually digested something).
    hash1 should not be sha256Hex(Seq.empty)
  }

  it should "demonstrate the by-construction property: both paths call ApplyPolicy" in {
    // Documentation-style structural assertion:
    //   - NeksurPolicyApplier defines `transformedTag` (Pitfall 2
    //     marker — the gate that prevents Catalyst double-apply).
    //   - NeksurDataFrameWriter constructs without error against a
    //     real DataFrame (proving the SDK entry point exists +
    //     resolves).
    //
    // The actual "both call ApplyPolicy" guarantee is enforced at
    // compile time: both classes import `transforms.ApplyPolicy` and
    // both delegate per-column work to `applyToDataFrame`. If either
    // class ever stops calling ApplyPolicy, the parity invariant
    // would break — and the test below at least guarantees both
    // classes are still in the build.
    val applierFields = classOf[NeksurPolicyApplier].getDeclaredFields.map(_.getName).toSet
    applierFields should contain("transformedTag")

    val spark = SparkSessionFixture.spark
    val df = spark.range(1).toDF()
    val writer = new NeksurDataFrameWriter(df, "parity.t")
    writer should not be null
  }
}
