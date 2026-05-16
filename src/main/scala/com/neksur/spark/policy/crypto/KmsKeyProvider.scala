/*
 * Neksur Spark Policy — AWS KMS GenerateDataKey wrapper with per-batch
 * in-process DEK cache (Pitfall 10 mitigation).
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 *
 * Licensed under the Business Source License 1.1 (BSL 1.1). The Change
 * Date is 2030-05-10; on the Change Date the rights granted in the
 * Change License (Apache License, Version 2.0) become effective.
 * See the LICENSE file at the repository root for the full license text.
 */

package com.neksur.spark.policy.crypto

import java.util.concurrent.ConcurrentHashMap

import software.amazon.awssdk.services.kms.KmsClient
import software.amazon.awssdk.services.kms.{model => kmsmodel}

import scala.jdk.CollectionConverters._

/**
 * Per-column data-encryption-key (DEK) cache, fronted by AWS KMS.
 *
 * == Pitfall 10 (RESEARCH §"Crypto Domain") ==
 *
 * Calling `kms.GenerateDataKey` once per output file (or, worse, per
 * row) crushes KMS quotas AND multiplies AWS bill. The contract is
 * '''one DEK per (tenant, column, batch)''', cached for the lifetime
 * of one Spark write batch (one `INSERT` / `MERGE` / `writeWithNeksur`
 * call). The Catalyst rule invalidates the cache via
 * [[clearCacheForBatch]] after the write commits — so successive
 * batches get fresh DEKs (forward secrecy on the per-batch granularity
 * D-2.07 settled on).
 *
 * == EncryptionContext ==
 *
 * Every `GenerateDataKey` carries an `EncryptionContext` map with
 * `neksur:tenant` + `neksur:column` keys. KMS bakes the context into
 * AAD; the same context is required to `Decrypt` the wrapped DEK.
 * This means a stolen ciphertext can't be decrypted by a process that
 * doesn't know (and lie about) its tenant + column origin —
 * defense-in-depth against the "compromised reader, wrong context"
 * threat in the Phase 2 threat register.
 *
 * == Why the `Kms` trait indirection ==
 *
 * `KmsClient` is an AWS SDK v2 interface with a wide surface area
 * (dozens of operations). Mocking it in tests requires either Mockito
 * (extra dep), LocalStack (testcontainer weight), or `java.lang.reflect.
 * Proxy` (gnarly). The minimal-surface `KmsKeyProvider.Kms` trait
 * lets tests inject a hand-rolled counting stub in 10 lines while
 * production wiring uses `RealKms(KmsClient.create())`.
 */
class KmsKeyProvider(kms: KmsKeyProvider.Kms, cmkArn: String) {

  // (plaintext, ciphertextBlob) tuple, retained until `clearCacheForBatch`.
  // ConcurrentHashMap is mandatory: a single Spark task on the driver may
  // share this provider across multiple executor-bound futures (e.g.
  // Catalyst rule fans out per-column transforms in parallel for wide
  // tables). Volatile-ish read+write needs a thread-safe map.
  private val cache = new ConcurrentHashMap[KmsKeyProvider.CacheKey, KmsKeyProvider.CachedDek]()

  /**
   * Derive (or fetch from cache) a 256-bit AES DEK for
   * `(tenantID, columnName, batchID)`.
   *
   * @return
   *   `(plaintextDek, encryptedDek)` — the plaintext is used IMMEDIATELY
   *   to encrypt column values and SHOULD be zeroized by the caller as
   *   soon as the column projection finishes (Pitfall 11 mitigation:
   *   plaintext DEK never written to disk; cache holds it ONLY for the
   *   lifetime of the batch). The encrypted blob travels in the sidecar
   *   manifest for later `Decrypt` calls by readers.
   */
  def deriveDataKey(
    tenantID: String,
    columnName: String,
    batchID: String
  ): (Array[Byte], Array[Byte]) = {
    val key = KmsKeyProvider.CacheKey(tenantID, columnName, batchID)
    val cached = cache.get(key)
    if (cached != null) {
      (cached.plaintext, cached.encrypted)
    } else {
      val (plaintext, encrypted) = kms.generateDataKey(
        cmkArn,
        Map(
          "neksur:column" -> columnName,
          "neksur:tenant" -> tenantID
        )
      )
      cache.put(key, KmsKeyProvider.CachedDek(plaintext, encrypted))
      (plaintext, encrypted)
    }
  }

  /**
   * Invalidate every cache entry whose `batchID` matches. Called by
   * the Catalyst rule's write-commit hook (Plan 02-07) — keeps the
   * cache from growing unbounded across long-lived driver sessions.
   *
   * Iteration over `ConcurrentHashMap.keys` is weakly consistent (per
   * the `ConcurrentHashMap` Javadoc); that's fine — we're only
   * collecting keys to remove, and a concurrent `put` for the SAME
   * batchID after `clearCacheForBatch` is, by definition, a bug in
   * the caller (the batch is supposed to be done).
   */
  def clearCacheForBatch(batchID: String): Unit = {
    val it = cache.keys()
    val toRemove = scala.collection.mutable.ArrayBuffer.empty[KmsKeyProvider.CacheKey]
    while (it.hasMoreElements) {
      val k = it.nextElement()
      if (k.batchID == batchID) toRemove += k
    }
    toRemove.foreach(cache.remove)
  }

  /** Test-only: number of currently cached DEKs. Never called from prod. */
  def cacheSize: Int = cache.size()
}

object KmsKeyProvider {

  /**
   * Minimal-surface KMS trait — one method, the only call
   * `KmsKeyProvider` ever makes against AWS. Test stubs implement
   * this directly; production wiring goes through [[RealKms]].
   *
   * @param cmkArn
   *   The CMK ARN to derive a DEK from. Pre-configured by the operator
   *   per tenant (or per environment, depending on the deployment
   *   model picked in Plan 02-09).
   *
   * @param encryptionContext
   *   AAD map baked into the DEK by KMS. Must be supplied verbatim
   *   on the corresponding `Decrypt` call by readers.
   *
   * @return
   *   `(plaintext, encryptedBlob)` — same contract as `GenerateDataKey`.
   */
  trait Kms {
    def generateDataKey(
      cmkArn: String,
      encryptionContext: Map[String, String]
    ): (Array[Byte], Array[Byte])
  }

  /**
   * Production wiring: wraps a real `KmsClient`. Stays in the
   * companion so the test source set has nothing AWS-y to mock.
   */
  class RealKms(client: KmsClient) extends Kms {
    override def generateDataKey(
      cmkArn: String,
      encryptionContext: Map[String, String]
    ): (Array[Byte], Array[Byte]) = {
      val req = kmsmodel.GenerateDataKeyRequest
        .builder()
        .keyId(cmkArn)
        .keySpec(kmsmodel.DataKeySpec.AES_256)
        .encryptionContext(encryptionContext.asJava)
        .build()
      val resp = client.generateDataKey(req)
      (resp.plaintext().asByteArray(), resp.ciphertextBlob().asByteArray())
    }
  }

  // Cache types — package-private so tests in the same package can
  // assert on them, but not part of the public API.

  private[crypto] final case class CacheKey(
    tenantID: String,
    columnName: String,
    batchID: String
  )

  private[crypto] final case class CachedDek(
    plaintext: Array[Byte],
    encrypted: Array[Byte]
  )
}
