/*
 * Neksur Spark Policy — NeksurDataFrameWriter SDK specs.
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 * Licensed under the Business Source License 1.1 (BSL 1.1).
 * See the LICENSE file at the repository root.
 */

package com.neksur.spark.policy

import com.neksur.spark.policy.NeksurDataFrameWriter.Implicits._
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * `NeksurDataFrameWriter` SDK coverage.
 *
 * Three tests that lock in the SDK's public contract:
 *
 *   1. The `writeWithNeksur` implicit on `DataFrame` is in scope and
 *      returns the right type — the entry point compiles + resolves.
 *   2. Fail-closed: when the policy fetch returns 5xx, calling
 *      `append()` must raise (the SDK MUST NOT silently degrade to a
 *      pass-through write). We don't assert the exact exception type
 *      because the Iceberg write itself will likely raise too (no
 *      local catalog) — what matters is *some* exception bubbles up
 *      whose chain contains the fail-closed marker
 *      "policy fetch failed".
 *   3. `merge()` raises `NotImplementedError` — Phase 2 stub, Plan
 *      02-08 lights up the real path. The stub MUST raise before
 *      issuing any HTTP fetch (verifiable here: no stub server is
 *      started in this test).
 *
 * Stub HTTP server: JDK `com.sun.net.httpserver.HttpServer` on an
 * ephemeral 127.0.0.1 port, torn down in `afterEach`. Same pattern
 * as `PolicyClientSpec` so the spec stays consistent + audit-friendly
 * (no third-party mock-server dependency).
 *
 * SparkSession lifetime: same comment as `NeksurPolicyApplierSpec` —
 * shared singleton, JVM exit handles teardown.
 */
class NeksurDataFrameWriterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var server: HttpServer = _

  override def afterEach(): Unit = {
    if (server != null) {
      server.stop(0)
      server = null
    }
    // Clean spark.neksur.* keys between tests so a 5xx-config from
    // one test does not leak into the next test's PolicyClient.
    val conf = SparkSessionFixture.spark.sparkContext.getConf
    conf.remove("spark.neksur.endpoint")
    conf.remove("spark.neksur.token")
    super.afterEach()
  }

  private def startStub(handler: HttpHandler): Int = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/v1/policy/transform-plan", handler)
    server.start()
    server.getAddress.getPort
  }

  private def respond(ex: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    ex.sendResponseHeaders(status, bytes.length.toLong)
    val os = ex.getResponseBody
    try os.write(bytes)
    finally os.close()
  }

  behavior of "NeksurDataFrameWriter"

  it should "expose writeWithNeksur via implicit" in {
    // The implicit `NeksurDataFrameOps` must be in scope from
    // `NeksurDataFrameWriter.Implicits._` and must yield a
    // `NeksurDataFrameWriter`. Construction is cheap — no HTTP
    // fetch fires until `append`/`overwrite`/`overwritePartitions`
    // is called.
    val df = SparkSessionFixture.spark.range(1).toDF()
    val w = df.writeWithNeksur("local.test.t")
    w shouldBe a[NeksurDataFrameWriter]
  }

  it should "fail-closed when policy fetch returns 5xx" in {
    // Stub returns 503 — PolicyClient surfaces a SparkException, the
    // SDK wraps it in another SparkException tagged "policy fetch
    // failed for table=...". We assert that marker bubbles up
    // through the cause chain (the Iceberg write below may add its
    // own exception on top because no local catalog is configured).
    val port = startStub((ex: HttpExchange) => respond(ex, 503, "service unavailable"))
    val conf = SparkSessionFixture.spark.sparkContext.getConf
    conf.set("spark.neksur.endpoint", s"http://127.0.0.1:$port")
    conf.set("spark.neksur.token", "test-jwt-do-not-use-in-prod")

    val df = SparkSessionFixture.spark.range(1).toDF()

    val thrown = intercept[Exception] {
      df.writeWithNeksur("ns.t").append()
    }

    // Walk the cause chain looking for the fail-closed marker.
    def messageChain(t: Throwable, acc: List[String] = Nil): List[String] = {
      val m = Option(t.getMessage).getOrElse("")
      if (t.getCause == null || t.getCause == t) (m :: acc).reverse
      else messageChain(t.getCause, m :: acc)
    }
    val msgs = messageChain(thrown)
    msgs.exists(_.contains("policy fetch failed")) shouldBe true
  }

  it should "throw NotImplementedError on merge()" in {
    // Phase 2 stub: merge() must raise *before* any HTTP fetch. We
    // deliberately do NOT start a stub server here — if merge()
    // touched the PolicyClient, the test would hang or fail with a
    // different exception (connection refused). The test passing
    // proves merge() is a hard stub, not an accidental pass-through.
    val df = SparkSessionFixture.spark.range(1).toDF()
    val w = df.writeWithNeksur("local.t")

    val thrown = intercept[NotImplementedError] { w.merge() }
    thrown.getMessage should include("merge")
  }
}
