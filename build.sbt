// Neksur Spark Policy — JVM library for L2 pre-write policy enforcement.
//
// Stack pin per /Users/evgeny/neksur/.planning/phases/02-.../02-RESEARCH.md
// §"Standard Stack" (JVM side) lines 124-178 + §"Validation Architecture" line 1159.
// License: BSL 1.1 (see LICENSE; Change Date 2030-05-10, Change License Apache 2.0).

name         := "neksur-spark-policy"
organization := "com.neksur"
version      := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.18"

// crossScalaVersions left as 2.12.18 only for Phase 2; Scala 2.13 cell is a
// future `sbt +test` matrix add — Spark 3.5 supports both binaries.
crossScalaVersions := Seq("2.12.18")

// `provided` scope — Spark + Iceberg ship inside the customer's Spark
// distribution; this jar is a thin layer on top of them (RESEARCH §line 129).
// `software.amazon.awssdk %% kms` is bundled because it is NOT in Spark's
// default classpath and is required for KMS GenerateDataKey from executors
// (decision D-2.07, RESEARCH §line 130, Phase 2 Plan 02-06 will exercise it).
libraryDependencies ++= Seq(
  "org.apache.spark"       %% "spark-sql"                      % "3.5.4"  % "provided",
  "org.apache.spark"       %% "spark-catalyst"                 % "3.5.4"  % "provided",
  "org.apache.iceberg"     %  "iceberg-spark-runtime-3.5_2.12" % "1.6.1"  % "provided",
  "software.amazon.awssdk" %  "kms"                            % "2.31.0",
  "org.scalatest"          %% "scalatest"                      % "3.2.18" % Test
)

// Fork a fresh JVM per test class so each test gets a clean SparkSession
// (SparkSession holds static singletons across the JVM lifetime).
// Required for the SmokeSpec + every future test that touches Spark.
// RESEARCH §line 1159: "Test / fork := true is required for SparkSession isolation".
Test / fork := true

// Generous heap for Spark in-process test sessions; matches local Spark 3.5
// defaults. Increase if integration tests OOM in CI.
Test / javaOptions ++= Seq(
  "-Xmx2g",
  "-Xss4m",
  // Spark on JDK 17 needs explicit module open-flags; ship them eagerly so
  // CI runners (java-version: 17 in .github/workflows/sbt-test.yml) succeed.
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

// Verbose ScalaTest output: classname + line on failure; surfaces stack
// traces from forked JVM cleanly.
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

// Compiler hygiene — fail builds on suspicious patterns. Will be tightened
// further in Plan 02-06 when real source lands.
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint:_",
  "-Ywarn-unused"
)

// Java compile target: 17 (Spark 3.5 supports JDK 8/11/17; 17 is the modern
// LTS pick — matches sbt-test.yml `java-version: 17`).
javacOptions ++= Seq("-source", "17", "-target", "17")

// Repository pointers: Maven Central is implicit; Apache Snapshots only if
// a future RC needs validating (commented out by default).
// resolvers += "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots/"

// License metadata for downstream Maven publish (Plan 02-08 wires the
// publish step; this block keeps the POM well-formed in the meantime).
licenses += ("Business Source License 1.1", url("https://mariadb.com/bsl11/"))
homepage  := Some(url("https://github.com/neksur-com/neksur-spark-policy"))
scmInfo   := Some(
  ScmInfo(
    url("https://github.com/neksur-com/neksur-spark-policy"),
    "scm:git:git@github.com:neksur-com/neksur-spark-policy.git"
  )
)
