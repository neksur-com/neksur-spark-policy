/*
 * Neksur Spark Policy — test fixture.
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 *
 * Licensed under the Business Source License 1.1 (BSL 1.1).
 * See the LICENSE file at the repository root.
 */

package com.neksur.spark.policy

import org.apache.spark.sql.SparkSession

/**
 * In-process local-mode `SparkSession` for tests.
 *
 * Per RESEARCH §"Validation Architecture" line 1159 + 1163: `Test / fork
 * := true` in `build.sbt` means each test class boots its own JVM, so
 * the singleton in this object is per-JVM-per-test-class — no
 * cross-test contamination. `stop()` is called from `afterAll` of every
 * suite that touches `spark`.
 *
 * The Iceberg `SparkSessionExtensions` config (`spark.sql.extensions`)
 * is wired so the iceberg-spark-runtime classes are discoverable on the
 * classpath; this is enough for the smoke test to construct an Iceberg
 * catalog reference. Plan 02-06 will add a real `local` catalog with a
 * temp-warehouse pointer for round-trip read/write tests.
 *
 * Phase 2 Plan 02-02 (this commit) ships the smallest fixture that
 * proves Spark + Iceberg + Scala 2.12.18 wire together; full Iceberg
 * catalog setup lands in Plan 02-06.
 */
object SparkSessionFixture {

  /**
   * Lazy singleton SparkSession. Boots on first access; tests that
   * never touch Spark pay zero cost.
   *
   * Configuration choices:
   *   - `local[2]`: 2 cores — parallel-but-bounded, fits CI runners
   *     (ubuntu-latest has 2 vCPUs as of 2026-05).
   *   - `spark.sql.extensions`: register Iceberg's Catalyst extension
   *     so `IcebergSparkSessionExtensions` is on the classpath. (No
   *     catalog is configured yet; Plan 02-06 adds the local catalog.)
   *   - `spark.sql.shuffle.partitions = 2`: keep test shuffles tiny.
   *   - `spark.ui.enabled = false`: no Spark UI in tests — port-bind
   *     conflicts between forked JVMs would otherwise be flaky.
   *   - `spark.driver.bindAddress = 127.0.0.1`: pin to loopback so the
   *     test never tries to bind to a NIC's public IP.
   */
  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .appName("neksur-spark-policy-smoke")
      .master("local[2]")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

  /**
   * Stop the singleton SparkSession. Idempotent — safe to call from
   * `afterAll` even if `spark` was never accessed (lazy val won't
   * initialize on `stop` alone).
   *
   * Implementation note: we check `SparkSession.getActiveSession`
   * before stopping to avoid triggering lazy-val initialization
   * inside afterAll just to immediately tear it down.
   */
  def stop(): Unit = {
    val active = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    active.foreach(_.stop())
  }
}
