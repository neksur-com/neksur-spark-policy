/*
 * Neksur Spark Policy — column-level encrypt transform (Phase 2 placeholder).
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 *
 * Licensed under the Business Source License 1.1 (BSL 1.1). The Change
 * Date is 2030-05-10; on the Change Date the rights granted in the
 * Change License (Apache License, Version 2.0) become effective.
 * See the LICENSE file at the repository root for the full license text.
 */

package com.neksur.spark.policy.transforms

import com.neksur.spark.policy.ColumnTransform
import org.apache.spark.SparkException
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, concat, lit, sha2, substring}
import org.apache.spark.sql.types.StringType

/**
 * Column-level encryption transform.
 *
 * == Phase 2 placeholder ==
 *
 * This object writes a DETERMINISTIC placeholder of the form
 * `"[ENCRYPTED:<sha256-prefix-of-cmk-and-value>]"` instead of real
 * ciphertext. The placeholder is sufficient to verify two invariants
 * the Phase 2 acceptance tests care about:
 *
 *   1. The transform IS applied — the plaintext column value is gone
 *      from the output (`ExtensionVsSdkParitySpec` and downstream
 *      acceptance specs assert no plaintext PII remains).
 *   2. Both the Catalyst (Extension) path and the SDK
 *      (`NeksurDataFrameWriter`) path produce IDENTICAL output bytes
 *      (`ExtensionVsSdkParitySpec` sha256-invariant). Because the
 *      placeholder is a pure function of `(cmk_arn, column-value)`, it
 *      is deterministic across the two paths by construction.
 *
 * == Why a placeholder, not hand-rolled AES-GCM ==
 *
 * Per RESEARCH §"Don't Hand-Roll" (line 713 of the Phase 2 research
 * doc): production code MUST NOT hand-roll AES-GCM or any other
 * cipher mode. The real implementation lands in Plan 02-08 alongside
 * the E2E acceptance test, and wires:
 *
 *   - Parquet modular encryption (`parquet.crypto.factory.class` + the
 *     `FileEncryptionProperties` native path), and
 *   - `KmsKeyProvider` DEK derivation (per-column DEK wrapped by the
 *     configured CMK; AES-256-GCM applied by the Parquet writer, not
 *     by application code).
 *
 * Shipping the placeholder in Phase 2 lets dispatch C wire the
 * SDK/Catalyst parity surface today without taking a dependency on
 * the Parquet-encryption native, which has its own integration test
 * matrix (Plan 02-08).
 */
object ColumnEncrypt {

  /**
   * Build the placeholder-encrypted column expression.
   *
   * Deterministic by construction: the same `(cmk_arn, value)` pair
   * always produces the same `[ENCRYPTED:<16-hex-chars>]` string.
   * Determinism is the property the parity spec relies on.
   *
   * The first 16 hex chars of `sha2(cmk || value, 256)` are sufficient
   * to make the placeholder visibly distinct from plaintext and to make
   * accidental collisions cryptographically negligible at Phase 2
   * scale; the real Plan 02-08 path replaces this with proper
   * ciphertext bytes inside the Parquet writer.
   */
  /**
   * WR-09: SparkConf key gating placeholder usage. The Phase 2
   * placeholder is fail-safe for privacy (no plaintext) but is a
   * data-integrity landmine — the `[ENCRYPTED:<sha256-prefix>]`
   * value is IRREVERSIBLE and a subsequent decrypt workflow cannot
   * recover the plaintext. Without a runtime gate, a customer who
   * deployed the Phase 2 jar into "production" would silently
   * destroy data. Require the operator to acknowledge via
   * `--conf spark.neksur.allow_placeholder_encrypt=true`.
   */
  val AllowPlaceholderConfKey: String = "spark.neksur.allow_placeholder_encrypt"

  def apply(df: DataFrame, t: ColumnTransform): Column = {
    // WR-09: refuse to apply the placeholder unless the operator
    // explicitly opts in. Plan 02-08 lands real Parquet modular
    // encryption + KmsKeyProvider DEK derivation; until then a
    // misconfigured job that reaches this code path with the gate
    // unset would emit an irreversible 16-hex-char string into the
    // customer's data, with no recovery path.
    if (df.sparkSession.sparkContext.getConf.get(AllowPlaceholderConfKey, "false") != "true") {
      throw new SparkException(
        s"ColumnEncrypt: Phase 2 placeholder IRREVERSIBLY destroys data — set " +
          s"$AllowPlaceholderConfKey=true to acknowledge that the column ${t.column} " +
          s"will be replaced with [ENCRYPTED:<sha256-prefix>] and cannot be decrypted. " +
          s"Plan 02-08 lands real Parquet modular encryption."
      )
    }
    val cmkArn = t.params.getOrElse("cmk_arn", "<unset>")
    // Placeholder shape: "[ENCRYPTED:" + first-16-hex-chars-of-sha256 + "]"
    concat(
      lit("[ENCRYPTED:"),
      substring(sha2(concat(lit(cmkArn), col(t.column).cast(StringType)), 256), 1, 16),
      lit("]")
    )
  }
}
