package com.github.joswlv.spark.cassandra.bulk

import org.apache.spark.rdd.RDD

import scala.reflect.runtime.universe._

package object rdd {

  /**
   * Implicitly lift a [[RDD]] with [[SparkCassRDDFunctions]].
   *
   * @param rdd A [[RDD]] to lift.
   * @return Enriched [[RDD]] with [[SparkCassRDDFunctions]].
   */
  implicit def sparkCassRDDFunctions[T: TypeTag](
    rdd: RDD[T]): SparkCassRDDFunctions[T] = {
    new SparkCassRDDFunctions[T](rdd)
  }
}
