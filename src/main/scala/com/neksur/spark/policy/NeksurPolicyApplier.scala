/*
 * Neksur Spark Policy — Catalyst Rule applying L2 transforms to V2 writes.
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 * Licensed under the Business Source License 1.1 (BSL 1.1).
 * See the LICENSE file at the repository root.
 */

package com.neksur.spark.policy

import com.neksur.spark.policy.transforms.ApplyPolicy
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.slf4j.LoggerFactory

/**
 * Catalyst optimizer rule that intercepts V2 writes to Neksur-governed
 * tables and rewrites them via transforms.ApplyPolicy.
 *
 * Pitfall 2 mitigation: TreeNodeTag marker prevents Catalyst's
 * fixed-point optimizer batch from double-applying the rule. Without
 * this marker, the optimizer would re-run the rule until plan
 * stabilizes, double-encrypting / double-masking on each pass.
 *
 * Fail-closed contract: any error in PolicyClient.fetchTransformPlan
 * raises SparkException via Try.get, which Catalyst surfaces as a
 * write failure (D-1.09 carried into JVM via SparkException).
 */
class NeksurPolicyApplier(spark: SparkSession) extends Rule[LogicalPlan] {
  private val log = LoggerFactory.getLogger(classOf[NeksurPolicyApplier])
  private val transformedTag = TreeNodeTag[Boolean]("neksur.transformed")

  // Lazy because PolicyClient is constructed from SparkConf at first
  // request, not at rule registration time (extension factory runs
  // before SparkConf is finalized in some Spark codepaths).
  private lazy val policyClient: PolicyClient = PolicyClient.fromSparkConf(spark.sparkContext.getConf)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case write if isV2WriteCommand(write) && !alreadyTransformed(write) && isNeksurGoverned(write) =>
        val tableRef = extractTableRef(write)
        log.info(s"NeksurPolicyApplier: intercepting write for table=$tableRef")
        val policy = policyClient.fetchTransformPlan(tableRef) match {
          case scala.util.Success(p) => p
          case scala.util.Failure(err) =>
            // Fail-closed: re-raise as SparkException so Catalyst aborts the write.
            throw new SparkException(
              s"NeksurPolicyApplier: policy fetch failed for table=$tableRef (write rejected)",
              err
            )
        }
        val rewritten = ApplyPolicy.apply(write, policy)
        rewritten.setTagValue(transformedTag, true)
        rewritten
    }
  }

  private def alreadyTransformed(p: LogicalPlan): Boolean =
    p.getTagValue(transformedTag).contains(true)

  /**
   * V2WriteCommand recognition — Iceberg 1.6.x ships AppendData /
   * OverwriteByExpression / OverwritePartitionsDynamic / ReplaceData
   * (per RESEARCH Pitfall 5 lines 730-755). Use class name string
   * matching to avoid an import-time dependency on a specific
   * sql/catalyst class hierarchy that varies across Spark minor
   * versions.
   */
  private def isV2WriteCommand(plan: LogicalPlan): Boolean = {
    val name = plan.getClass.getSimpleName
    name == "AppendData" || name == "OverwriteByExpression" ||
      name == "OverwritePartitionsDynamic" || name == "ReplaceData"
  }

  /**
   * Phase 2 simplification: governed-tables list comes from SparkConf
   * (`spark.neksur.governed_tables` — comma-separated qualified names).
   * Phase 3 will switch to a dynamic discovery query against the
   * control plane.
   *
   * WR-06 fail-closed: if extractTableRef cannot resolve the plan's
   * target table (e.g., a Spark minor version renamed the field) we
   * raise a SparkException rather than silently treating the write as
   * non-governed. The previous swallow-Throwable behavior was a
   * fail-open path — any unexpected Spark internal change made every
   * write skip policy enforcement, with only a warn log as audit.
   */
  private def isNeksurGoverned(plan: LogicalPlan): Boolean = {
    val configured = spark.sparkContext.getConf.get("spark.neksur.governed_tables", "")
    if (configured.isEmpty) return false
    val governed = configured.split(",").map(_.trim).filter(_.nonEmpty).toSet
    // No try/catch — extractTableRef now raises SparkException on
    // failure, which the Catalyst rule call site surfaces as a write
    // failure (fail-closed posture).
    val ref = extractTableRef(plan)
    governed.contains(ref)
  }

  /**
   * Best-effort tableRef extraction across V2WriteCommand subclasses.
   * Each subclass exposes the target table differently; reflection-
   * based lookup avoids a hard import-time dependency on Spark's
   * sql/catalyst types.
   *
   * WR-06: every error path raises a SparkException so callers can
   * fail closed. Returning `plan.toString` was a fail-open footgun —
   * a Spark minor-version rename of `table` → `tableInfo` would have
   * silently routed every governed write through the non-governed
   * path with zero alerting.
   *
   * Returns the qualified name as a string — the canonical form that
   * spark.neksur.governed_tables stores.
   */
  private def extractTableRef(plan: LogicalPlan): String = {
    val tableMethod = plan.getClass.getMethods.find(_.getName == "table")
      .getOrElse {
        throw new SparkException(
          s"NeksurPolicyApplier: cannot extract tableRef from plan " +
            s"${plan.getClass.getName} — no 'table' method (Spark version skew?)")
      }
    try {
      val tbl = tableMethod.invoke(plan)
      if (tbl == null) {
        throw new SparkException(
          s"NeksurPolicyApplier: plan ${plan.getClass.getName} returned null tableRef")
      }
      // tbl may be NamedRelation with `name` method.
      val nameMethod = tbl.getClass.getMethods.find(_.getName == "name")
      nameMethod match {
        case Some(nm) =>
          val n = nm.invoke(tbl)
          if (n == null) {
            throw new SparkException(
              s"NeksurPolicyApplier: plan ${plan.getClass.getName} returned null name")
          }
          n.toString
        case None => tbl.toString
      }
    } catch {
      case se: SparkException => throw se
      case t: Throwable =>
        throw new SparkException(
          s"NeksurPolicyApplier: reflection failed extracting tableRef from " +
            s"${plan.getClass.getName}: ${t.getMessage}", t)
    }
  }
}
