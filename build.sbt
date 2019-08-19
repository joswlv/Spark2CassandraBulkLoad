import scalariform.formatter.preferences._

/**
 * Organization:
 */
organization := "com.github.joswlv"
organizationName := "SeungwanJo"

/**
 * Library Meta:
 */
name := "Spark2CassandraBulkLoad"
licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))

version := "1.0.0"
scalaVersion := "2.11.12"

/**
 * Library Dependencies:
 */

// Versions:
val sparkVersion = "[2.0, 3.0["
val sparkTestVersion = "[2.0_0.12.0, 3.0["
val scalaTestVersion = "3.1.0-RC1"
val cassandraAllVersion = "[3.0, 4.0["
val cassandraUnitVersion = "[3.0, 4.0["

// Dependencies:
val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % Provided
val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
val ssc = "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion
val cassandraAll = "org.apache.cassandra" % "cassandra-all" % cassandraAllVersion

val sparkTest = "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests"
val sparkSqlTest = "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests"
val catalystTest = "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests"
val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % Test
val scalactic = "org.scalactic" %% "scalactic" % scalaTestVersion % Test
val cassandraUnit = "org.cassandraunit" % "cassandra-unit" % cassandraUnitVersion % Test

libraryDependencies ++= Seq(sparkCore, sparkSql, ssc, cassandraAll, sparkTest, sparkSqlTest, catalystTest, scalaTest, scalactic, cassandraUnit)
excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12")
  , ExclusionRule("io.netty", "netty ")
)

/**
 * Tests:
 */
parallelExecution in Test := false

/**
 * Scalariform:
 */
scalariformPreferences := scalariformPreferences.value
  .setPreference(RewriteArrowSymbols, false)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(SpacesAroundMultiImports, true)

scalariformAutoformat := true