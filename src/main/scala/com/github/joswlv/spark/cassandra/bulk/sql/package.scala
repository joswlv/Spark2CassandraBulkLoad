package com.github.joswlv.spark.cassandra.bulk

import org.apache.spark.sql.DataFrame

package object sql {
  implicit def sparkCassDataFrameFunctions(df: DataFrame): SparkCassDataFrameFunctions = {
    new SparkCassDataFrameFunctions(df)
  }
}
