package com.github.joswlv.spark.cassandra.bulk.sql

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{RowWriterFactory, SqlRowWriter}
import com.datastax.spark.connector.{AllColumns, ColumnSelector}
import com.github.joswlv.spark.cassandra.bulk.SparkCassandraBulkWriter
import com.github.joswlv.spark.cassandra.bulk.conf.SparkCassWriteConf
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Extension of [[DataFrame]] with [[bulkLoadToCass()]] function.
 *
 * @param dataFrame The [[DataFrame]] to lift into extension.
 */
class SparkCassDataFrameFunctions(dataFrame: DataFrame) extends Serializable {
  /**
   * SparkContext to schedule [[com.github.joswlv.spark.cassandra.bulk.SparkCassandraBulkWriter]] Tasks.
   */
  private[sql] val internalSparkContext = dataFrame.sqlContext.sparkContext

  /**
   * Loads the data from [[DataFrame]] to a Cassandra table. Uses the specified column names.
   *
   * Writes SSTables to a temporary directory then loads the SSTables directly to Cassandra.
   *
   * @param keyspaceName The name of the Keyspace to use.
   * @param tableName The name of the Table to use.
   * @param columns The list of column names to save data to.
   *                Uses only the unique column names, and you must select all primary key columns.
   *                All other fields are discarded. Non-selected property/column names are left unchanged.
   * @param sparkCassWriteConf Configurations to coordinate and to limit writes.
   * @param sparkCassWriteConf Configurations connection
   */
  def bulkLoadToCass(
    keyspaceName:       String,
    tableName:          String,
    columns:            ColumnSelector     = AllColumns,
    sparkCassWriteConf: SparkCassWriteConf = SparkCassWriteConf.fromSparkConf(internalSparkContext.getConf)
  )(implicit
    connector: CassandraConnector = CassandraConnector(internalSparkContext.getConf),
    rwf: RowWriterFactory[Row] = SqlRowWriter.Factory): Unit = {
    val sparkCassandraBulkWriter = SparkCassandraBulkWriter(
      connector,
      keyspaceName,
      tableName,
      columns,
      sparkCassWriteConf
    )

    internalSparkContext.runJob(dataFrame.rdd, sparkCassandraBulkWriter.write _)
  }
}
