/*
 * Neksur Spark Policy — explicit SDK wrapper (Approach A, D-2.06).
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 * Licensed under the Business Source License 1.1 (BSL 1.1).
 * See the LICENSE file at the repository root.
 */

package com.neksur.spark.policy

import com.neksur.spark.policy.transforms.ApplyPolicy
import org.apache.spark.SparkException
import org.apache.spark.sql.{Column, DataFrame}
import scala.util.{Failure, Success}

/**
 * Explicit SDK wrapper for teams that prefer opt-in over the silent
 * Catalyst extension. Both surfaces call the same transforms.ApplyPolicy
 * function so the identical-snapshot invariant holds by construction
 * (D-2.06 + ExtensionVsSdkParitySpec proof).
 *
 * Fail-closed: PolicyClient.fetchTransformPlan failure raises
 * SparkException via Try.get; the calling pipeline aborts.
 */
class NeksurDataFrameWriter private[policy] (df: DataFrame, table: String) {
  private lazy val policyClient: PolicyClient =
    PolicyClient.fromSparkConf(df.sparkSession.sparkContext.getConf)

  def append(): Unit = {
    val transformed = transformedDf()
    transformed.writeTo(table).append()
  }

  def overwrite(condition: Column): Unit = {
    val transformed = transformedDf()
    transformed.writeTo(table).overwrite(condition)
  }

  def overwritePartitions(): Unit = {
    val transformed = transformedDf()
    transformed.writeTo(table).overwritePartitions()
  }

  /**
   * MERGE — Phase 2 stub. D-2.06 + Plan 02-08 will exercise MERGE
   * end-to-end when the live Iceberg testfixture lands; this dispatch
   * raises NotImplementedError so callers get a clear failure mode
   * rather than a silent pass-through.
   */
  def merge(): Unit = {
    throw new NotImplementedError(
      "NeksurDataFrameWriter.merge is deferred to Plan 02-08 — use append/overwrite for Phase 2"
    )
  }

  private def transformedDf(): DataFrame = {
    val policy = policyClient.fetchTransformPlan(table) match {
      case Success(p) => p
      case Failure(err) =>
        throw new SparkException(
          s"NeksurDataFrameWriter: policy fetch failed for table=$table (write rejected)",
          err
        )
    }
    ApplyPolicy.applyToDataFrame(df, policy)
  }
}

object NeksurDataFrameWriter {
  /**
   * Implicit conversion — `df.writeWithNeksur(table)` syntax.
   */
  object Implicits {
    implicit class NeksurDataFrameOps(val df: DataFrame) extends AnyVal {
      def writeWithNeksur(table: String): NeksurDataFrameWriter =
        new NeksurDataFrameWriter(df, table)
    }
  }
}
