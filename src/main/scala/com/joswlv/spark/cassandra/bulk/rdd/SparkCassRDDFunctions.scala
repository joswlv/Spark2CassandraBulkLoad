package com.joswlv.spark.cassandra.bulk.rdd

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{ DefaultRowWriter, RowWriterFactory }
import com.datastax.spark.connector.{ AllColumns, ColumnSelector, toRDDFunctions, _ }
import com.joswlv.spark.cassandra.bulk.SparkCassandraBulkWriter
import com.joswlv.spark.cassandra.bulk.conf.SparkCassWriteConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Extension of [[RDD]] with [[bulkLoadToCass()]] function.
 *
 * @param rdd The [[RDD]] to lift into extension.
 */
class SparkCassRDDFunctions[T: TypeTag](rdd: RDD[T]) extends Serializable {
  /**
   * SparkContext to schedule [[SparkCassandraBulkWriter]] Tasks.
   */
  private[rdd] val internalSparkContext = rdd.sparkContext

  implicit private val classTag: ClassTag[T] = ClassTag[T](typeTag[T].mirror.runtimeClass(typeTag[T].tpe))

  /**
   * Loads the data from [[RDD]] to a Cassandra table. Uses the specified column names.
   *
   * Writes SSTables to a temporary directory then loads the SSTables directly to Cassandra.
   *
   * @param keyspaceName       The name of the Keyspace to use.
   * @param tableName          The name of the Table to use.
   * @param columns            The list of column names to save data to.
   *                           Uses only the unique column names, and you must select all primary key columns.
   *                           All other fields are discarded. Non-selected property/column names are left unchanged.
   * @param sparkCassWriteConf Configurations connection
   */
  def bulkLoadToCass(
    keyspaceName: String,
    tableName: String,
    columns: ColumnSelector = AllColumns,
    sparkCassWriteConf: SparkCassWriteConf = SparkCassWriteConf.fromSparkConf(internalSparkContext.getConf))(implicit connector: CassandraConnector = CassandraConnector(internalSparkContext.getConf),
      rwf: RowWriterFactory[T] = DefaultRowWriter.factory[T]): Unit = {

    if (classTag.equals(ClassTag(classOf[Row]))) {
      val sparkCassandraBulkWriter =
        SparkCassandraBulkWriter[CassandraRow](connector, keyspaceName, tableName, columns, sparkCassWriteConf)
      val rowFieldNames = rdd.first().asInstanceOf[Row].schema.fieldNames.toSet
      val tableFieldNames = sparkCassandraBulkWriter.columnNames.toSet

      require(hasValidColumnNames(rowFieldNames, tableFieldNames), s"Row RDD must have same column names [$rowFieldNames] to Cassandra table columns [$tableFieldNames]")

      internalSparkContext.runJob(rdd.map { case r: Row => CassandraRow.fromMap(r.getValuesMap(r.schema.fieldNames)) }
        .repartitionByCassandraReplica(keyspaceName, tableName, 1), sparkCassandraBulkWriter.write _)
      rdd.map { case r: Row => CassandraRow.fromMap(r.getValuesMap(r.schema.fieldNames)) }
    } else {
      val sparkCassandraBulkWriter = SparkCassandraBulkWriter(
        connector, keyspaceName, tableName, columns, sparkCassWriteConf)
      internalSparkContext.runJob(rdd.repartitionByCassandraReplica(keyspaceName, tableName, 1), sparkCassandraBulkWriter.write _)
    }
  }

  private def hasValidColumnNames(rowColumns: Set[String], tableColumns: Set[String]) = {
    rowColumns == tableColumns
  }
}
