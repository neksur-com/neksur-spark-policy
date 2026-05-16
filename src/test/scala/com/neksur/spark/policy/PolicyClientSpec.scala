/*
 * Neksur Spark Policy — PolicyClient HTTP behavior tests.
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 *
 * Licensed under the Business Source License 1.1 (BSL 1.1).
 * See the LICENSE file at the repository root.
 */

package com.neksur.spark.policy

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.time.Duration

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.{SparkConf, SparkException}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success}

/**
 * `PolicyClient` HTTP behavior coverage.
 *
 * Strategy: stand up `com.sun.net.httpserver.HttpServer` (JDK builtin,
 * zero extra dep) on an ephemeral port per test, serve canned responses,
 * point `PolicyClient` at it. Each test tears down its own server in
 * `afterEach` to avoid port leaks across forked-JVM test runs.
 *
 * Why JDK `HttpServer` and not WireMock / Mock-Server? The same reason
 * we don't pull in a third-party HTTP client for `PolicyClient` itself:
 * keeping the Phase 2 build artifact small + audit-friendly. JDK
 * `HttpServer` is "experimental" per the Javadoc but has been stable
 * since JDK 6 and is widely used in test fixtures.
 */
class PolicyClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  // Reset between tests so each one stops its own server cleanly.
  private var server: HttpServer = _

  override def afterEach(): Unit = {
    if (server != null) {
      // 0 → stop immediately; tests don't depend on graceful shutdown.
      server.stop(0)
      server = null
    }
    super.afterEach()
  }

  /**
   * Stand up an `HttpServer` on an OS-chosen ephemeral port with the
   * supplied handler bound at `/v1/policy/transform-plan`. Returns the
   * actual bound port so the test can construct `endpoint =
   * s"http://127.0.0.1:$port"`.
   */
  private def startStub(handler: HttpHandler): Int = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/v1/policy/transform-plan", handler)
    server.start()
    server.getAddress.getPort
  }

  /**
   * Tiny helper to send a body + status from a handler. Centralizes
   * the boilerplate so test handlers stay one-liner-ish.
   */
  private def respond(ex: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    ex.sendResponseHeaders(status, bytes.length.toLong)
    val os = ex.getResponseBody
    try os.write(bytes)
    finally os.close()
  }

  behavior of "PolicyClient"

  it should "parse a happy-path response" in {
    val port = startStub((ex: HttpExchange) => {
      val body =
        """{"tableRef":"ns.t","transforms":[{"column":"ssn","kind":"mask","params":{"format":"XXX-XX-LAST4"}}]}"""
      respond(ex, 200, body)
    })

    val client = new PolicyClient(
      endpoint = s"http://127.0.0.1:$port",
      token = "test-jwt-do-not-use-in-prod",
      timeout = Duration.ofSeconds(2)
    )

    client.fetchTransformPlan("ns.t") match {
      case Success(p) =>
        p.tableRef shouldBe "ns.t"
        p.transforms should have size 1
        val ct = p.transforms.head
        ct.column shouldBe "ssn"
        ct.kind shouldBe TransformKind.Mask
        ct.params shouldBe Map("format" -> "XXX-XX-LAST4")
      case Failure(err) =>
        fail(s"expected Success, got Failure: $err")
    }
  }

  it should "Failure on 5xx" in {
    val port = startStub((ex: HttpExchange) => respond(ex, 503, "service unavailable"))

    val client = new PolicyClient(
      endpoint = s"http://127.0.0.1:$port",
      token = "test-jwt-do-not-use-in-prod",
      timeout = Duration.ofSeconds(2)
    )

    val res = client.fetchTransformPlan("ns.t")
    res.isFailure shouldBe true
    val err = res.failed.get
    err shouldBe a[SparkException]
    err.getMessage should include("HTTP 503")
    // Pitfall 14: bearer token MUST NOT appear in the error message.
    err.getMessage should not include "test-jwt-do-not-use-in-prod"
  }

  it should "Failure on malformed JSON" in {
    val port = startStub((ex: HttpExchange) => respond(ex, 200, "not json"))

    val client = new PolicyClient(
      endpoint = s"http://127.0.0.1:$port",
      token = "test-jwt-do-not-use-in-prod",
      timeout = Duration.ofSeconds(2)
    )

    val res = client.fetchTransformPlan("ns.t")
    res.isFailure shouldBe true
    val err = res.failed.get
    err shouldBe a[SparkException]
    err.getMessage should include("malformed")
    err.getMessage should not include "test-jwt-do-not-use-in-prod"
  }

  it should "Failure on connection refused" in {
    // Port 1 is in the system-reserved range and (almost certainly) has
    // nothing listening on it, so the OS returns ECONNREFUSED.
    val client = new PolicyClient(
      endpoint = "http://127.0.0.1:1",
      token = "test-jwt-do-not-use-in-prod",
      timeout = Duration.ofSeconds(1)
    )

    val res = client.fetchTransformPlan("ns.t")
    res.isFailure shouldBe true
    val err = res.failed.get
    err shouldBe a[SparkException]
    err.getMessage should include("fetch failed")
    err.getMessage should not include "test-jwt-do-not-use-in-prod"
  }

  // ---------------------------------------------------------------
  // WR-A7 regression coverage — fromSparkConf must raise
  // SparkException (NOT IllegalArgumentException) on missing config,
  // matching the NeksurPolicyApplier class-level fail-closed contract.
  // ---------------------------------------------------------------

  behavior of "PolicyClient.fromSparkConf"

  it should "throw SparkException when spark.neksur.endpoint is missing" in {
    val conf = new SparkConf(loadDefaults = false)
      .set(PolicyClient.TokenConfKey, "tkn")
    val ex = intercept[SparkException] {
      PolicyClient.fromSparkConf(conf)
    }
    ex.getMessage should include("endpoint")
  }

  it should "throw SparkException when spark.neksur.token is missing" in {
    val conf = new SparkConf(loadDefaults = false)
      .set(PolicyClient.EndpointConfKey, "http://127.0.0.1:0")
    val ex = intercept[SparkException] {
      PolicyClient.fromSparkConf(conf)
    }
    ex.getMessage should include("token")
  }
}
