// Neksur Spark Policy — sbt plugins.
//
// Pinned per Phase 2 RESEARCH §"Security Domain V10" line 1220
// (Malicious Code Defense: sbt-dependency-check for SBOM + CVE scan)
// and §"Standard Stack JVM" Pitfall hardening.
//
// Plugin choices justified:
//
// - sbt-dependency-check: OWASP Dependency-Check wrapper. Surfaces CVEs in
//   transitive deps; runs in CI alongside `sbt test`. Threat T-2-jvm-deps-
//   supplychain in plan's threat register. Group ID corrected per the
//   plugin's published coordinates ("net.vonbuchholtz" :: "sbt-dependency-
//   check"); version 5.1.0 is the current 5.x line (Phase 2 acceptance:
//   plugin parses; Plan 02-06+ wires `dependencyCheck` task into CI).
//
// - sbt-scoverage: coverage reporting; gating threshold deferred to
//   Plan 02-06 once real code lands (per CONTEXT.md note "real
//   implementation lands in Plan 02-06").
//
// - sbt-mima-plugin: binary-compat checker. Critical because Spark
//   Extensions ship as customer-deployed jars; ABI breaks bite hard.
//   Activation deferred to Plan 02-08 (first published jar gets the
//   baseline) — declared here so Plan 02-08 doesn't need a plugins.sbt
//   PR.
//
// All three plugins are dormant in Phase 2 Plan 02-02 (this plan); they
// only activate when their respective tasks (`dependencyCheck`,
// `coverage`, `mimaReportBinaryIssues`) are explicitly invoked.

addSbtPlugin("net.vonbuchholtz" % "sbt-dependency-check" % "5.1.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.11")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.3")
