/*
 * Neksur Spark Policy — KmsKeyProvider cache + EncryptionContext tests.
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 *
 * Licensed under the Business Source License 1.1 (BSL 1.1).
 * See the LICENSE file at the repository root.
 */

package com.neksur.spark.policy

import com.neksur.spark.policy.crypto.KmsKeyProvider
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * `KmsKeyProvider` behavior:
 *
 *   1. Per-batch caching (Pitfall 10 mitigation — one KMS call per
 *      (tenant, column, batch), not per row / per file).
 *   2. Cache key includes `batchID` (different batches → different
 *      KMS calls even for the same column).
 *   3. EncryptionContext map carries `neksur:tenant` + `neksur:column`
 *      (AAD propagation requirement — readers MUST present the same
 *      context to `Decrypt`).
 *
 * Test stub: hand-rolled `Kms` impl (the minimal-surface trait in
 * `KmsKeyProvider`'s companion). No Mockito, no LocalStack —
 * 10 lines of stub for full behavioral coverage of the dispatch.
 */
class KmsKeyProviderSpec extends AnyFlatSpec with Matchers {

  /**
   * Counting + capturing stub. Records the number of `generateDataKey`
   * calls AND the encryption context of the last invocation, so the
   * EncryptionContext propagation test can assert on it without
   * adding any extra plumbing to the production `Kms` trait.
   */
  private class CountingKms extends KmsKeyProvider.Kms {
    var calls: Int = 0
    var lastContext: Map[String, String] = Map.empty
    var lastCmkArn: String = ""

    override def generateDataKey(
      cmkArn: String,
      encryptionContext: Map[String, String]
    ): (Array[Byte], Array[Byte]) = {
      calls += 1
      lastContext = encryptionContext
      lastCmkArn = cmkArn
      // 32-byte plaintext + 64-byte ciphertext blob — matches the
      // shape AWS KMS returns for a `DataKeySpec.AES_256` request.
      (Array.fill[Byte](32)(0x42.toByte), Array.fill[Byte](64)(0x43.toByte))
    }
  }

  private val TestCmk = "arn:aws:kms:us-east-1:000000000000:key/test"

  behavior of "KmsKeyProvider"

  it should "cache per (tenant, column, batch)" in {
    val stub = new CountingKms
    val provider = new KmsKeyProvider(stub, TestCmk)

    // Same (tenant, column, batch) repeated 5x → one KMS call total.
    (1 to 5).foreach { _ =>
      val (pt, ct) = provider.deriveDataKey("tenant-A", "ssn", "batch-1")
      pt.length shouldBe 32
      ct.length shouldBe 64
    }

    stub.calls shouldBe 1
    provider.cacheSize shouldBe 1
  }

  it should "miss cache for different batchID" in {
    val stub = new CountingKms
    val provider = new KmsKeyProvider(stub, TestCmk)

    provider.deriveDataKey("tenant-A", "ssn", "batch-A")
    provider.deriveDataKey("tenant-A", "ssn", "batch-B")

    stub.calls shouldBe 2
    provider.cacheSize shouldBe 2

    // And clearing one batch leaves the other intact.
    provider.clearCacheForBatch("batch-A")
    provider.cacheSize shouldBe 1
  }

  it should "include EncryptionContext with neksur:column and neksur:tenant" in {
    val stub = new CountingKms
    val provider = new KmsKeyProvider(stub, TestCmk)

    provider.deriveDataKey("tenant-A", "ssn", "batch-1")

    stub.calls shouldBe 1
    stub.lastCmkArn shouldBe TestCmk
    stub.lastContext.keySet should contain allOf ("neksur:column", "neksur:tenant")
    stub.lastContext("neksur:column") shouldBe "ssn"
    stub.lastContext("neksur:tenant") shouldBe "tenant-A"
  }
}
