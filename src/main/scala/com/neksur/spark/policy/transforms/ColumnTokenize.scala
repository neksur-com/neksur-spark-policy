/*
 * Neksur Spark Policy — column-level deterministic tokenization transform.
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
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, concat, lit, sha2, substring}
import org.apache.spark.sql.types.StringType

/**
 * Deterministic, format-preserving-ish tokenization.
 *
 * Token = first 16 hex characters of
 * `SHA-256(tenant_salt_id || column_name || value)`. The result is a
 * stable 16-char hex string per `(salt, column, value)` triple.
 *
 * == Determinism guarantee ==
 *
 * Same input value AND same `tenant_salt_id` → same token. This is
 * the property that lets downstream JOINs survive tokenization:
 * two tables both tokenizing `customer_id` with the same salt still
 * join correctly. (Cross-tenant joins are intentionally broken by
 * design — each tenant gets its own salt.)
 *
 * SHA-256 collision probability at 16-hex-char (64-bit) truncation
 * is cryptographically negligible at Phase 2 scale (2^32-row birthday
 * bound is still ~4B distinct values per column per tenant before a
 * 50% collision probability — well above any single-tenant table
 * cardinality we'll see in Phase 2).
 *
 * == Phase 2 vs Phase 6 ==
 *
 * Phase 2 (this file): in-process SHA-256 with the salt ID baked into
 * the policy params. The salt VALUE is not actually fetched here —
 * the ID is the only input — because we don't yet have an HSM-backed
 * salt store. This is a deliberate simplification: it's deterministic
 * and the property tests can prove the joinability invariant, but it
 * is NOT a production-grade tokenization service.
 *
 * Phase 6 will wire a real format-preserving-encryption (FPE) service
 * with an HSM-backed master key; the policy params will switch from
 * `tenant_salt_id` to a service endpoint + key reference.
 */
object ColumnTokenize {

  /**
   * Build the tokenized-column expression. The salt ID is read from
   * `t.params("tenant_salt_id")`, defaulting to `"default"` if absent
   * (so a misconfigured policy still produces a stable token rather
   * than failing the write — fail-safe over fail-closed here, since
   * the column still gets transformed away from plaintext).
   */
  def apply(df: DataFrame, t: ColumnTransform): Column = {
    val _ = df // reserved for future schema-aware tokenization
    val tenantSaltID = t.params.getOrElse("tenant_salt_id", "default")
    // Token = first 16 hex chars of sha256(salt || column || value)
    substring(
      sha2(concat(lit(tenantSaltID), lit(t.column), col(t.column).cast(StringType)), 256),
      1,
      16
    )
  }
}
