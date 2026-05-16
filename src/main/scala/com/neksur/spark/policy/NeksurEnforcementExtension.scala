/*
 * Neksur Spark Policy — SparkSessionExtensions entry point.
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 * Licensed under the Business Source License 1.1 (BSL 1.1).
 * See the LICENSE file at the repository root.
 */

package com.neksur.spark.policy

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Spark Extension entry point — register via:
 *
 *   --conf spark.sql.extensions=com.neksur.spark.policy.NeksurEnforcementExtension
 *
 * Per RESEARCH §Code Example 4 lines 954-960 + Assumption A9:
 * `injectOptimizerRule` (NOT `injectPostHocResolutionRule`) — Iceberg's
 * own rules use the same injection point so timing matches V2WriteCommand
 * interception.
 */
class NeksurEnforcementExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { spark =>
      new NeksurPolicyApplier(spark)
    }
  }
}
