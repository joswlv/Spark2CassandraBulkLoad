import scalariform.formatter.preferences._

/**
* Organization:
*/
organization     := "com.github.joswlv"
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
val SparkVersion                   = "2.3.2"
val SparkTestVersion               = "2.3.1_0.10.0"
val ScalaTestVersion               = "3.0.5"
val SparkCassandraConnectorVersion = "2.3.2"
val CassandraAllVersion            = "3.11.3"
val CassandraUnitVersion           = "3.5.0.1"

// Dependencies:
val sparkCore       = "org.apache.spark"     %% "spark-core"                % SparkVersion                   % "provided"
val sparkSql        = "org.apache.spark"     %% "spark-sql"                 % SparkVersion                   % "provided"
val sparkTest       = "com.holdenkarau"      %% "spark-testing-base"        % SparkTestVersion               % "test"
val scalaTest       = "org.scalatest"        %% "scalatest"                 % ScalaTestVersion               % "test"
val ssc             = "com.datastax.spark"   %% "spark-cassandra-connector" % SparkCassandraConnectorVersion
val cassandraAll    = "org.apache.cassandra" %  "cassandra-all"             % CassandraAllVersion
val cassandraUnit   = "org.cassandraunit"    %  "cassandra-unit"            % CassandraUnitVersion           % "test"

libraryDependencies ++= Seq(sparkCore, sparkSql, sparkTest, scalaTest, ssc, cassandraAll, cassandraUnit)

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