package com.github.joswlv.spark.cassandra.bulk.rdd

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.mapper.ColumnMapper
import com.datastax.spark.connector.writer.{DefaultRowWriter, RowWriterFactory}
import com.datastax.spark.connector.{AllColumns, ColumnSelector}
import com.github.joswlv.spark.cassandra.bulk.SparkCassandraBulkWriter
import com.github.joswlv.spark.cassandra.bulk.conf.SparkCassWriteConf
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.universe._

/**
 * Extension of [[RDD]] with [[bulkLoadToCass()]] function.
 *
 * @param rdd The [[RDD]] to lift into extension.
 */
class SparkCassRDDFunctions[T: ColumnMapper: TypeTag](rdd: RDD[T]) extends Serializable {
  /**
   * SparkContext to schedule [[com.github.joswlv.spark.cassandra.bulk.SparkCassandraBulkWriter]] Tasks.
   */
  private[rdd] val internalSparkContext = rdd.sparkContext

  /**
   * Loads the data from [[RDD]] to a Cassandra table. Uses the specified column names.
   *
   * Writes SSTables to a temporary directory then loads the SSTables directly to Cassandra.
   *
   * @param keyspaceName The name of the Keyspace to use.
   * @param tableName The name of the Table to use.
   * @param columns The list of column names to save data to.
   *                Uses only the unique column names, and you must select all primary key columns.
   *                All other fields are discarded. Non-selected property/column names are left unchanged.
   * @param sparkCassWriteConf Configurations connection
   */
  def bulkLoadToCass(
    keyspaceName:       String,
    tableName:          String,
    columns:            ColumnSelector     = AllColumns,
    sparkCassWriteConf: SparkCassWriteConf = SparkCassWriteConf.fromSparkConf(internalSparkContext.getConf)
  )(implicit
    connector: CassandraConnector = CassandraConnector(internalSparkContext.getConf),
    rwf: RowWriterFactory[T] = DefaultRowWriter.factory[T]): Unit = {
    val sparkCassandraBulkWriter = SparkCassandraBulkWriter(
      connector,
      keyspaceName,
      tableName,
      columns,
      sparkCassWriteConf
    )

    internalSparkContext.runJob(rdd, sparkCassandraBulkWriter.write _)
  }
}
