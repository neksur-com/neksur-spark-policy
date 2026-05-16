/*
 * Neksur Spark Policy — HTTP client for the control-plane policy
 * decision endpoint (`/v1/policy/transform-plan`).
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 *
 * Licensed under the Business Source License 1.1 (BSL 1.1). The Change
 * Date is 2030-05-10; on the Change Date the rights granted in the
 * Change License (Apache License, Version 2.0) become effective.
 * See the LICENSE file at the repository root for the full license text.
 */

package com.neksur.spark.policy

import java.net.URI
import java.net.URLEncoder
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.Duration

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * Fetches per-table transform plans from the Neksur control plane.
 *
 * == Design constraints ==
 *
 *   - '''Zero third-party HTTP dep.''' Uses `java.net.http.HttpClient`
 *     from JDK 17 stdlib; no Akka HTTP, no sttp, no okhttp pulled in.
 *     This keeps the published jar tiny (Spark customers already
 *     scrutinize executor classpath bloat).
 *
 *   - '''Zero third-party JSON dep.''' Walks `JsonNode` from
 *     `jackson-databind`, which Spark already drags in transitively.
 *     We do NOT register `jackson-module-scala` (not guaranteed on the
 *     classpath) — manual `JsonNode` traversal keeps us portable.
 *
 *   - '''Fail-closed.''' Every error path — network timeout, HTTP 5xx,
 *     malformed JSON, unknown transform kind — returns
 *     `Try.Failure[SparkException]`. The Catalyst rule short-circuits
 *     on Failure so writes fail closed rather than silently shipping
 *     plaintext (Pitfall 1 mitigation).
 *
 *   - '''Bearer token never logged.''' Every log + exception message is
 *     audited to NOT interpolate the token (defense against Pitfall 14:
 *     accidental token leak via log aggregation). Token only appears in
 *     the `Authorization: Bearer ...` header on the outgoing request.
 *
 * == Lifecycle ==
 *
 * Each `PolicyClient` lazily owns one `HttpClient` (connection pool).
 * Reuse the same instance across multiple `fetchTransformPlan` calls
 * — construction cost is non-trivial. The companion `fromSparkConf`
 * builder is the canonical wiring entry point used by the Catalyst
 * extension (Plan 02-06 dispatch C).
 */
class PolicyClient(
  endpoint: String,
  token: String,
  timeout: Duration = Duration.ofSeconds(5)
) {

  // Per-instance logger; classOf[PolicyClient] resolves the standard
  // `com.neksur.spark.policy.PolicyClient` SLF4J name.
  // SECURITY: NEVER `log.warn(s"... $token ...")` — the token MUST stay
  // out of logs. See class-level docs.
  private val log = LoggerFactory.getLogger(classOf[PolicyClient])

  // Lazy so test instances that never make a request don't pay
  // the cost of setting up a connection pool.
  private lazy val client: HttpClient =
    HttpClient.newBuilder().connectTimeout(timeout).build()

  // Reused per instance; `ObjectMapper` is thread-safe AFTER configuration
  // (Jackson's documented invariant). No Scala module registration —
  // we walk `JsonNode` manually to avoid depending on jackson-module-scala
  // being on the executor classpath.
  private val mapper: ObjectMapper = new ObjectMapper()

  /**
   * Fetch the transform plan for `tableRef` from the control plane.
   *
   * @param tableRef
   *   Dotted qualifier (`"namespace.table"` or just `"table"`); passed
   *   verbatim as the `?table=` query string parameter. Callers are
   *   responsible for URL-safety; current control-plane impl rejects
   *   anything that isn't a valid Iceberg table identifier server-side.
   *
   * @return
   *   `Success(Policy)` on HTTP 2xx with parseable body; `Failure(
   *   SparkException)` on any error path. We deliberately use
   *   `SparkException` (not a custom exception) so the Catalyst rule's
   *   `try/catch` matches one type and produces a Spark-friendly
   *   stack trace in the driver log.
   */
  def fetchTransformPlan(tableRef: String): Try[Policy] = {
    // WR-07: URL-encode the tableRef query parameter. Without encoding,
    // a tableRef containing '&', '#', '?' would split the query string
    // (control plane reads only the substring up to the first '&'),
    // and a tableRef containing '+' or '%' would be silently
    // misinterpreted. The control-plane handler validates the parameter
    // server-side, but a defense-in-depth client-side encode ensures
    // the right value reaches the server in the first place.
    val encodedTableRef = URLEncoder.encode(tableRef, StandardCharsets.UTF_8)
    val req = HttpRequest
      .newBuilder()
      .uri(URI.create(s"$endpoint/v1/policy/transform-plan?table=$encodedTableRef"))
      .header("Authorization", s"Bearer $token")
      .timeout(timeout)
      .GET()
      .build()

    Try(client.send(req, HttpResponse.BodyHandlers.ofString())) match {
      case Failure(err) =>
        // SECURITY: message + structured log argument MUST NOT include `token`.
        log.warn("PolicyClient: fetch failed", err)
        Failure(
          new SparkException(
            s"PolicyClient: fetch failed for table=$tableRef: ${err.getMessage}"
          )
        )

      case Success(resp) =>
        val status = resp.statusCode()
        if (status >= 400) {
          // Body intentionally NOT included in the error message — control plane
          // 4xx responses may echo policy fragments that we'd rather not bleed
          // into a Spark driver log line read by every operator on the cluster.
          Failure(
            new SparkException(
              s"PolicyClient: HTTP $status for table=$tableRef"
            )
          )
        } else {
          parseBody(resp.body(), tableRef)
        }
    }
  }

  // --- internal: JSON → ADT ----------------------------------------------

  /**
   * Parse a control-plane response body into a [[Policy]]. Returns
   * `Failure(SparkException)` on ANY malformed input — fail-closed.
   *
   * Expected wire shape:
   * {{{
   * {
   *   "tableRef": "ns.tbl",
   *   "transforms": [
   *     {"column": "ssn",   "kind": "mask",   "params": {"format": "XXX-XX-LAST4"}},
   *     {"column": "email", "kind": "redact", "params": {}}
   *   ]
   * }
   * }}}
   *
   * Notes:
   *   - `transforms[]` may be empty (legal: "no L2 transforms for this table").
   *   - `params` may be missing or `null` → treated as empty `Map`.
   *   - Unknown `kind` strings cause Failure (Pitfall 6: fail-closed on
   *     unknown transform vs. silently dropping it).
   */
  private def parseBody(body: String, tableRef: String): Try[Policy] = {
    Try {
      val root: JsonNode = mapper.readTree(body)
      if (root == null || root.isNull) {
        throw new RuntimeException("empty body")
      }
      val tableNode = root.get("tableRef")
      if (tableNode == null) throw new RuntimeException("missing field: tableRef")
      val table = tableNode.asText()

      val arr = root.get("transforms")
      val transforms: Seq[ColumnTransform] =
        if (arr == null || arr.isNull) Seq.empty
        else {
          if (!arr.isArray) throw new RuntimeException("transforms is not an array")
          (0 until arr.size()).map { i =>
            val n = arr.get(i)
            val columnNode = n.get("column")
            if (columnNode == null) throw new RuntimeException(s"transforms[$i] missing column")
            val column = columnNode.asText()

            val kindNode = n.get("kind")
            if (kindNode == null) throw new RuntimeException(s"transforms[$i] missing kind")
            val kindStr = kindNode.asText()
            val kind = TransformKind
              .fromString(kindStr)
              .getOrElse(throw new RuntimeException(s"unknown kind: $kindStr"))

            val paramsNode = n.get("params")
            val params: Map[String, String] =
              if (paramsNode == null || paramsNode.isNull) Map.empty[String, String]
              else {
                val it = paramsNode.fields()
                val b = scala.collection.mutable.Map.empty[String, String]
                while (it.hasNext) {
                  val e = it.next()
                  b += (e.getKey -> e.getValue.asText())
                }
                b.toMap
              }
            ColumnTransform(column, kind, params)
          }
        }
      Policy(table, transforms)
    }.recoverWith { case err =>
      Failure(
        new SparkException(
          s"PolicyClient: malformed response for table=$tableRef: ${err.getMessage}"
        )
      )
    }
  }
}

/**
 * Companion: SparkConf-driven construction.
 *
 * Reads three config keys (Plan 02-06 dispatch C wires these via
 * `--conf spark.neksur.*` on `spark-submit`):
 *
 *   - `spark.neksur.endpoint` (required) — base URL of the control plane
 *     (e.g. `https://control.neksur.example`). Trailing `/` is NOT
 *     stripped — caller responsibility.
 *
 *   - `spark.neksur.token` (required) — bearer token for the
 *     `/v1/policy/*` API. SHOULD be sourced from a secret manager and
 *     injected via Spark's secret-redaction config, not committed to
 *     a job's `--conf` literal.
 *
 *   - `spark.neksur.timeout_seconds` (optional, default 5) — request
 *     timeout. Override via `--conf` for slow control planes; values
 *     <1 are clamped to 1 second (a 0-second timeout would race-fail).
 */
object PolicyClient {

  // Single source of truth for config key strings. Reuse from
  // KmsKeyProvider + Catalyst extension wiring once those land.
  val EndpointConfKey: String = "spark.neksur.endpoint"
  val TokenConfKey: String    = "spark.neksur.token"
  val TimeoutConfKey: String  = "spark.neksur.timeout_seconds"

  /**
   * Build a `PolicyClient` from a `SparkConf`. Throws
   * `IllegalArgumentException` if `endpoint` or `token` is missing —
   * jobs that try to enable the extension without configuring the
   * control plane MUST fail fast at startup (not at first write).
   */
  def fromSparkConf(conf: SparkConf): PolicyClient = {
    val endpoint = conf.getOption(EndpointConfKey).getOrElse {
      throw new IllegalArgumentException(
        s"$EndpointConfKey is required to enable Neksur policy enforcement"
      )
    }
    val token = conf.getOption(TokenConfKey).getOrElse {
      throw new IllegalArgumentException(
        s"$TokenConfKey is required to enable Neksur policy enforcement"
      )
    }
    val timeoutSec = conf
      .getOption(TimeoutConfKey)
      .flatMap(s => scala.util.Try(s.toLong).toOption)
      .getOrElse(5L)
      .max(1L)
    new PolicyClient(endpoint, token, Duration.ofSeconds(timeoutSec))
  }
}
