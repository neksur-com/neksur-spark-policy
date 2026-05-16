/*
 * Neksur Spark Policy — ApplyPolicy + per-transform unit tests.
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 *
 * Licensed under the Business Source License 1.1 (BSL 1.1).
 * See the LICENSE file at the repository root.
 */

package com.neksur.spark.policy

import com.neksur.spark.policy.transforms.ApplyPolicy
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for `ApplyPolicy.applyToDataFrame` covering all four
 * `TransformKind` variants plus a multi-transform composition case.
 *
 * The SDK path (`applyToDataFrame`) is what `ExtensionVsSdkParitySpec`
 * (dispatch C) compares against the Catalyst path. Exercising it here
 * directly proves the per-transform semantics in isolation before the
 * full parity spec lands; if a transform regresses, this file is the
 * first thing to catch it.
 *
 * `Test / fork := true` in `build.sbt` means each spec gets its own
 * forked JVM, so `SparkSessionFixture.stop()` in `afterAll` is enough
 * to tear down cleanly without contaminating the next class.
 */
class ApplyPolicySpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

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
  // Mask
  // ----------------------------------------------------------------- //

  "ApplyPolicy.applyToDataFrame" should "mask SSN columns with XXX-XX-LAST4" in {
    import spark.implicits._

    val df = Seq("123-45-6789", "987-65-4321").toDF("ssn")
    val policy = Policy(
      "test.ssn_table",
      Seq(
        ColumnTransform(
          column = "ssn",
          kind = TransformKind.Mask,
          params = Map("format" -> "XXX-XX-LAST4")
        )
      )
    )

    val out = ApplyPolicy.applyToDataFrame(df, policy)
    val rows = out.collect().map(_.getString(0)).toSet

    rows should contain theSameElementsAs Set("XXX-XX-6789", "XXX-XX-4321")
  }

  // ----------------------------------------------------------------- //
  // Redact — default NULL
  // ----------------------------------------------------------------- //

  it should "redact columns with NULL by default" in {
    import spark.implicits._

    val df = Seq("alice@example.com", "bob@example.org").toDF("email")
    val policy = Policy(
      "test.email_table",
      Seq(
        ColumnTransform(
          column = "email",
          kind = TransformKind.Redact,
          params = Map.empty
        )
      )
    )

    val out = ApplyPolicy.applyToDataFrame(df, policy)
    val rows = out.collect()

    rows should have length 2
    all(rows.map(_.isNullAt(0))) shouldBe true
  }

  // ----------------------------------------------------------------- //
  // Redact — literal
  // ----------------------------------------------------------------- //

  it should "redact with literal when redact_with is set" in {
    import spark.implicits._

    val df = Seq("alice@example.com", "bob@example.org").toDF("email")
    val policy = Policy(
      "test.email_table",
      Seq(
        ColumnTransform(
          column = "email",
          kind = TransformKind.Redact,
          params = Map("redact_with" -> "REDACTED")
        )
      )
    )

    val out = ApplyPolicy.applyToDataFrame(df, policy)
    val rows = out.collect().map(_.getString(0)).toSet

    rows shouldBe Set("REDACTED")
  }

  // ----------------------------------------------------------------- //
  // Encrypt — deterministic placeholder
  // ----------------------------------------------------------------- //

  it should "encrypt with deterministic placeholder" in {
    import spark.implicits._

    val df = Seq("plaintext-a", "plaintext-b").toDF("payload")
    val policy = Policy(
      "test.payload_table",
      Seq(
        ColumnTransform(
          column = "payload",
          kind = TransformKind.Encrypt,
          params = Map("cmk_arn" -> "arn:aws:kms:us-east-1:111:key/abc")
        )
      )
    )

    val out1 = ApplyPolicy.applyToDataFrame(df, policy).collect().map(_.getString(0))
    val out2 = ApplyPolicy.applyToDataFrame(df, policy).collect().map(_.getString(0))

    // Determinism: same (df, policy) twice ⇒ identical output.
    out1.toSeq shouldBe out2.toSeq
    // Shape: every cell matches the placeholder pattern.
    val placeholderRegex = """\[ENCRYPTED:[0-9a-f]{16}\]""".r
    all(out1.map(s => placeholderRegex.findFirstIn(s).isDefined)) shouldBe true
    // Plaintext is gone.
    out1 should not contain "plaintext-a"
    out1 should not contain "plaintext-b"
  }

  // ----------------------------------------------------------------- //
  // Tokenize — deterministic per (salt, column, value)
  // ----------------------------------------------------------------- //

  it should "tokenize deterministically per (salt, column, value)" in {
    import spark.implicits._

    val df = Seq("user-001", "user-002", "user-001").toDF("uid")
    val policy = Policy(
      "test.uid_table",
      Seq(
        ColumnTransform(
          column = "uid",
          kind = TransformKind.Tokenize,
          params = Map("tenant_salt_id" -> "t1")
        )
      )
    )

    val out1 = ApplyPolicy.applyToDataFrame(df, policy).collect().map(_.getString(0))
    val out2 = ApplyPolicy.applyToDataFrame(df, policy).collect().map(_.getString(0))

    // Determinism across two runs.
    out1.toSeq shouldBe out2.toSeq
    // Length invariant: 16 hex chars per token.
    val hex16 = """[0-9a-f]{16}""".r
    all(out1.map(s => hex16.findFirstIn(s).isDefined)) shouldBe true
    // Same plaintext ⇒ same token: the two "user-001" rows must collide.
    out1(0) shouldBe out1(2)
    // Different plaintext ⇒ different token (with overwhelming probability).
    out1(0) should not be out1(1)
  }

  // ----------------------------------------------------------------- //
  // Composition: multiple transforms in policy.transforms order
  // ----------------------------------------------------------------- //

  it should "compose multiple transforms in policy.transforms order" in {
    import spark.implicits._

    val df = Seq(("123-45-6789", "alice@example.com")).toDF("ssn", "email")
    val policy = Policy(
      "test.compose_table",
      Seq(
        ColumnTransform(
          column = "ssn",
          kind = TransformKind.Mask,
          params = Map("format" -> "XXX-XX-LAST4")
        ),
        ColumnTransform(
          column = "email",
          kind = TransformKind.Redact,
          params = Map("redact_with" -> "REDACTED")
        )
      )
    )

    val out = ApplyPolicy.applyToDataFrame(df, policy).collect()

    out should have length 1
    out(0).getString(0) shouldBe "XXX-XX-6789"
    out(0).getString(1) shouldBe "REDACTED"
  }
}
