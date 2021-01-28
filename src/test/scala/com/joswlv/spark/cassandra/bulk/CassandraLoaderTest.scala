package com.joswlv.spark.cassandra.bulk

import com.joswlv.spark.cassandra.bulk.rdd._
import com.joswlv.spark.cassandra.bulk.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.SharedSparkSession
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.FunSuite

class CassandraLoaderTest extends FunSuite with SharedSparkSession {
  val cassandraSession = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra()
    EmbeddedCassandraServerHelper.getSession
  }

  override def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.cassandra.connection.port", "9142")
    conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val loader = new CQLDataLoader(EmbeddedCassandraServerHelper.getSession)
    loader.load(new FileCQLDataSet(getClass.getResource("/create.sql").getPath))
  }

  private def getInputData(sampleFileName: String): DataFrame = {
    spark.read
      .option("header", "true")
      .csv(getClass.getResource(s"/$sampleFileName").getPath)
  }

  private def bulkLoadAndCompare(keyspace: String, table: String, sampleFileName: String): Unit = {
    val df = getInputData(sampleFileName)
    df.bulkLoadToCass(keyspace, table)
    checkCassandraData(keyspace, table, df.count().toInt)
  }

  private def checkCassandraData(keyspace: String, table: String, expectedCount: Int): Unit = {
    val result = cassandraSession.execute(s"select * from $keyspace.$table")
    assertResult(expectedCount) {
      result.all().size()
    }
  }

  test("string only table test") {
    bulkLoadAndCompare("test", "string_only", "simple_data.csv")
  }

  test("row type rdd test") {
    val keyspace = "test"
    val table = "rdd_row"
    val rowRDD = getInputData("simple_data.csv").rdd
    rowRDD.bulkLoadToCass(keyspace, table)
    checkCassandraData(keyspace, table, rowRDD.count().toInt)
  }

  test("row type with schema rdd test") {
    val keyspace = "test"
    val table = "rdd_row"
    val rowRDD = getInputData("simple_data.csv")
      .toDF("id", "date", "value").rdd
    rowRDD.bulkLoadToCass(keyspace, table)
    checkCassandraData(keyspace, table, rowRDD.count().toInt)
  }

  test("row type with schema rdd diff order test") {
    val keyspace = "test"
    val table = "rdd_row"
    val rowRDD = getInputData("simple_data2.csv").rdd
    rowRDD.bulkLoadToCass(keyspace, table)
    checkCassandraData(keyspace, table, rowRDD.count().toInt)
  }

  test("not row type rdd test") {
    val keyspace = "test"
    val table = "rdd_not_row"
    val tupleRDD = getInputData("simple_data.csv").rdd
      .map(row => (row.getString(0), row.getString(1), row.getString(2)))
    tupleRDD.bulkLoadToCass(keyspace, table)
    checkCassandraData(keyspace, table, tupleRDD.count().toInt)
  }

  test("numeric field table test") {
    bulkLoadAndCompare("test", "numeric", "numeric_data.csv")
  }

  test("date field table test") {
    bulkLoadAndCompare("test", "date_type", "date_data.csv")
  }
}