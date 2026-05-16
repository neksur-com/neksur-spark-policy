/*
 * Neksur Spark Policy — shared case classes (Policy, ColumnTransform,
 * TransformKind, TableRef).
 *
 * Copyright (c) 2026 Neksur. All rights reserved.
 *
 * Licensed under the Business Source License 1.1 (BSL 1.1). The Change
 * Date is 2030-05-10; on the Change Date the rights granted in the
 * Change License (Apache License, Version 2.0) become effective.
 * See the LICENSE file at the repository root for the full license text.
 */

package com.neksur.spark.policy

/**
 * Sealed taxonomy of column-level transforms the L2 enforcer can apply
 * to a `V2WriteCommand` projection.
 *
 * The four variants intentionally cover the Phase 2 RESEARCH §"Standard
 * Stack" transform menu (masking / format-preserving redaction /
 * encryption with KMS-derived DEKs / format-preserving tokenization).
 * Pattern matches MUST be exhaustive — the `sealed` keyword guarantees
 * the compiler enforces this at every call site (Pitfall 6 mitigation:
 * adding a new transform kind without updating every dispatch site is a
 * compile error, not a runtime surprise).
 *
 * Wire format: lower-case string ("mask" / "encrypt" / "redact" /
 * "tokenize") matching the JSON the control plane emits in its
 * `/v1/policy/transform-plan` response. `fromString` is the single
 * parse point used by `PolicyClient` to translate wire → ADT.
 */
sealed trait TransformKind

object TransformKind {

  /** Static masking — replace value with a fixed-width placeholder. */
  case object Mask extends TransformKind

  /**
   * Envelope encryption using a per-column DEK derived from the
   * configured CMK (`KmsKeyProvider`). The encrypted ciphertext is
   * written in place of the plaintext; the wrapped DEK travels in
   * sidecar metadata (Plan 02-07 wires the sidecar writer).
   */
  case object Encrypt extends TransformKind

  /** Drop the value entirely (replace with `NULL` of the column type). */
  case object Redact extends TransformKind

  /**
   * Deterministic, format-preserving tokenization. Same plaintext →
   * same token (within a tenant) so joins survive. Implementation
   * lands in Plan 02-06 dispatch B alongside the other transforms.
   */
  case object Tokenize extends TransformKind

  /**
   * Parse a lower-case wire string to the corresponding ADT variant.
   * Returns `None` for any unrecognized kind — the caller (typically
   * `PolicyClient`) is responsible for converting `None` into a hard
   * failure (fail-closed: an unknown transform kind MUST NOT silently
   * pass plaintext through to disk).
   */
  def fromString(s: String): Option[TransformKind] = s.toLowerCase match {
    case "mask"     => Some(Mask)
    case "encrypt"  => Some(Encrypt)
    case "redact"   => Some(Redact)
    case "tokenize" => Some(Tokenize)
    case _          => None
  }
}

/**
 * Identifier for a target table: `namespace.name`. The namespace may
 * be empty (top-level table); `qualifiedName` returns the dotted form
 * when non-empty, otherwise just `name`.
 *
 * `parse` splits on the LAST `.` so multi-segment namespaces (e.g.
 * `catalog.schema.table`) preserve the full prefix as `namespace`.
 */
final case class TableRef(namespace: String, name: String) {
  def qualifiedName: String =
    if (namespace.isEmpty) name else s"$namespace.$name"
}

object TableRef {

  /**
   * Parse `"a.b.c"` → `TableRef("a.b", "c")`, `"t"` → `TableRef("", "t")`.
   * The last `.` is the separator — multi-segment catalogs stay intact
   * in the namespace field.
   */
  def parse(s: String): TableRef = {
    val idx = s.lastIndexOf('.')
    if (idx < 0) TableRef("", s) else TableRef(s.substring(0, idx), s.substring(idx + 1))
  }
}

/**
 * One column-level transform directive.
 *
 * `params` is a free-form key/value bag — each transform kind defines
 * its own schema:
 *   - `Mask`: `format` → e.g. `"XXX-XX-LAST4"`
 *   - `Encrypt`: `cmk_arn` (optional override), `keyspec` (optional)
 *   - `Redact`: no params required
 *   - `Tokenize`: `tenant_salt_id`, `algorithm`
 *
 * Validation of param presence/well-formedness happens in the per-kind
 * applier (Plan 02-06 dispatch B) — `ColumnTransform` is just transport.
 */
final case class ColumnTransform(
  column: String,
  kind: TransformKind,
  params: Map[String, String]
)

/**
 * The full policy for one table — what the control plane returns from
 * `/v1/policy/transform-plan?table=...`. `tableRef` is the dotted
 * qualifier of the target table (matches what was requested);
 * `transforms` is the ordered list of column-level directives.
 *
 * Empty `transforms` is legal — it means "no L2 transforms apply to
 * this table, write through untouched". The Catalyst rule still
 * inspects every write and treats an empty plan as "pass-through OK"
 * (vs. a fetch failure, which fails the write closed).
 */
final case class Policy(
  tableRef: String,
  transforms: Seq[ColumnTransform]
)
