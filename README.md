# Spark2CassandraBulkLoad

Spark Library for Bulk Loading into Cassandra

This project refers to [Spark2Cassandra](https://github.com/jparkie/Spark2Cassandra)

Upgrade utility(spark, cassandra) version.

## Features

1. Convert rdd or dataframe to SSTableFile.
2. Stream the SSTableFile to Cassandra nodes.

- Build Status(To be added)

## Requirements

Spark2CassandraBulkLoad supports Spark 2.x and above.

| Spark2Cassandra Version | Cassandra Version | JDK Version |
| ------------------------| ----------------- | ----------- |
| `1.X.X`                 | `3.0.0+`          | 1.8+        |

## Downloads

#### SBT

```scala
libraryDependencies += "com.joswlv.spark.cassandra.bulk" %% "Spark2CassandraBulkLoad" % "1.0.1"
```

#### Maven
```xml
<dependency>
	<groupId>com.joswlv.spark.cassandra.bulk</groupId>
	<artifactId>Spark2CassandraBulkLoad</artifactId>
	<version>1.0.1</version>
</dependency>
```

### gradle

```groovy
compile 'com.joswlv.spark.cassandra.bulk:Spark2CassandraBulkLoad:1.0.1'
```

## Usage

### Bulk Loading into Cassandra

```scala
// Import the following to have access to the `bulkLoadToEs()` function for RDDs or DataFrames.
import com.joswlv.spark.cassandra.bulk.rdd._
import com.joswlv.spark.cassandra.bulk.sql._

// Specify the `keyspaceName` and the `tableName` to write.
rdd.bulkLoadToCass(
  keyspaceName = "keyspaceName",
  tableName = "tableName"
)

// Specify the `keyspaceName` and the `tableName` to write.
df.bulkLoadToCass(
  keyspaceName = "keyspaceName",
  tableName = "tableName"
)
```