/*
 * Neksur Spark Policy — column-level redact transform (NULL or literal).
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
import org.apache.spark.sql.functions.lit

/**
 * Column-level redaction transform.
 *
 * Default behavior: replace the column's value with `NULL` cast to the
 * column's existing data type. The cast is important — `lit(null)`
 * without a cast widens the column type to `NullType`, which would
 * break downstream readers that expect e.g. `StringType` or `LongType`.
 *
 * If `params("redact_with")` is provided, the literal value is used
 * instead of NULL — useful when the downstream consumer cannot
 * tolerate NULLs (e.g. a non-nullable Parquet column) and a stable
 * sentinel like `"REDACTED"` is acceptable.
 *
 * Pure function: returns a Spark `Column` expression. The caller
 * (`ApplyPolicy.applyToDataFrame`) wraps with `df.withColumn`.
 */
object ColumnRedact {

  /**
   * Build the redacted-column expression. `df` is used to read the
   * column's existing `DataType` so the NULL replacement preserves
   * schema width — the resulting column has the same type as before
   * the transform, just with NULL values.
   */
  def apply(df: DataFrame, t: ColumnTransform): Column = {
    t.params.get("redact_with") match {
      case Some(literal) =>
        // Literal sentinel — every row gets exactly this value.
        lit(literal)
      case None =>
        // Cast NULL to the column's existing type so the schema
        // doesn't widen to NullType under us.
        val dtype = df.schema(t.column).dataType
        lit(null).cast(dtype)
    }
  }
}
