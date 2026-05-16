/*
 * Neksur Spark Policy — smoke test.
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 *
 * Licensed under the Business Source License 1.1 (BSL 1.1).
 * See the LICENSE file at the repository root.
 */

package com.neksur.spark.policy

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Phase 2 Plan 02-02 smoke test.
 *
 * Proves three things:
 *
 *   1. The sbt build compiles, ScalaTest discovers tests, and the
 *      forked JVM can load classes from `com.neksur.spark.policy`.
 *   2. The package object `Version` constant is reachable from test
 *      code (sanity-check that the `src/main/scala` tree links into
 *      the test classpath).
 *   3. A `SparkSession` can be constructed via `SparkSessionFixture`
 *      and basic DataFrame operations work end-to-end on the
 *      Spark 3.5.4 + iceberg-spark-runtime 1.6.1 + Scala 2.12.18
 *      cell pinned in `build.sbt`.
 *
 * Plan 02-06 will replace this with real Extension / SDK / ApplyPolicy
 * tests; this file is the smallest viable proof that the build works.
 *
 * Note on `BeforeAndAfterAll`: a single forked JVM per test class
 * (Test / fork := true) means the SparkSession started in `beforeAll`
 * is torn down in `afterAll` and re-created fresh for the next class.
 */
class SmokeSpec extends AnyFunSuite with BeforeAndAfterAll {

  // Stable reference to the SparkSession started in beforeAll;
  // tests read from this rather than re-touching the lazy val.
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

  test("Version constant is readable from package object") {
    assert(policy.Version.nonEmpty, "Version must be set")
    assert(policy.Version == "0.1.0-SNAPSHOT", s"Expected 0.1.0-SNAPSHOT, got ${policy.Version}")
  }

  test("SparkSession boots and a small DataFrame can be counted") {
    // `import spark.implicits._` enables `Seq(...).toDF` — without it
    // the implicit conversion isn't in scope inside the test body.
    import spark.implicits._

    val df = Seq(1, 2, 3).toDF("n")
    assert(df.count() == 3L, "DataFrame from Seq(1,2,3) must have 3 rows")
  }
}
