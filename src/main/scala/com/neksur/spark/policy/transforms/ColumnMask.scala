/*
 * Neksur Spark Policy — column-level static masking transform.
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
import org.apache.spark.sql.functions.{col, concat, lit, substring, substring_index}

/**
 * Static, format-preserving column masks.
 *
 * Pure function: returns a Spark `Column` expression that replaces the
 * column's value with a masked form. The caller
 * (`ApplyPolicy.applyToDataFrame`) wraps the result in
 * `df.withColumn(t.column, ColumnMask.apply(df, t))`.
 *
 * Supported `format` parameters (`t.params("format")`):
 *   - `"XXX-XX-LAST4"` — SSN-style: replace all but the last 4
 *     characters with `XXX-XX-`. Surfaces enough of the value to
 *     correlate with downstream lookups (e.g. customer service) while
 *     stripping the discriminating prefix.
 *   - `"EMAIL-DOMAIN-ONLY"` — replace the local-part of an email
 *     address with `REDACTED`, preserving the `@domain` so per-domain
 *     analytics (corporate vs. personal email, geographic TLD
 *     distribution) still work.
 *   - any other string — used as a static literal replacement (no
 *     escape required; if the policy says "REDACTED", the cell holds
 *     "REDACTED" verbatim).
 *
 * Missing `format` defaults to `"REDACTED"` (matches the most common
 * fail-safe and matches the wire-format default the control plane
 * emits when no format is configured).
 */
object ColumnMask {

  /**
   * Build the masked-column expression. `df` is passed in (rather than
   * just the column name) so future formats that need the column's
   * type or other schema-derived info can read it without re-plumbing
   * the call sites.
   */
  def apply(df: DataFrame, t: ColumnTransform): Column = {
    val _ = df // reserved for future schema-aware formats
    val format = t.params.getOrElse("format", "REDACTED")
    format match {
      case "XXX-XX-LAST4" =>
        // SSN-style mask: prefix "XXX-XX-" + the last 4 characters of
        // the original value. Negative start in `substring` is 1-based
        // from the end (Spark semantics), so `-4, 4` = last 4 chars.
        concat(lit("XXX-XX-"), substring(col(t.column), -4, 4))

      case "EMAIL-DOMAIN-ONLY" =>
        // Replace local-part with REDACTED, keep `@domain`.
        // `substring_index(s, "@", -1)` returns the substring AFTER
        // the last `@` (i.e. the domain). Prepend "REDACTED@" to
        // rebuild a valid-shape address.
        concat(lit("REDACTED@"), substring_index(col(t.column), "@", -1))

      case _ =>
        // Static literal mask — every row gets exactly this value.
        lit(format)
    }
  }
}
