package com.github.joswlv.spark.cassandra.bulk.conf

import com.datastax.driver.core.DataType
import com.datastax.spark.connector.cql.{ ColumnDef, RegularColumn }
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.writer.{ PerRowWriteOptionValue, TTLOption, TimestampOption, WriteOption }
import com.github.joswlv.spark.cassandra.bulk.util.SparkCassConfParam
import org.apache.cassandra.dht.{ ByteOrderedPartitioner, IPartitioner, Murmur3Partitioner, RandomPartitioner }
import org.apache.spark.SparkConf

/**
 * Configurations to coordinate and to limit the performance of writes.
 *
 * @param partitioner        The 'partitioner' defined in cassandra.yaml.
 * @param throughputMiBPS    The maximum throughput to throttle.
 * @param connectionsPerHost The number of connections per host to utilize when streaming SSTables.
 * @param ttl                The default TTL value which is used when it is defined (in seconds).
 * @param timestamp          The default timestamp value which is used when it is defined (in microseconds).
 */
case class SparkCassWriteConf(
  partitioner:        String          = SparkCassWriteConf.SPARK_CASSANDRA_BULK_WRITE_PARTITIONER.default,
  throughputMiBPS:    Int             = SparkCassWriteConf.SPARK_CASSANDRA_BULK_WRITE_THROUGHPUT_MB_PER_SEC.default,
  connectionsPerHost: Int             = SparkCassWriteConf.SPARK_CASSANDRA_BULK_WRITE_CONNECTIONS_PER_HOST.default,
  keySpaceUserName:   String          = SparkCassWriteConf.SPARK_CASSANDRA_BULK_WRITE_KEYSPACE_USER_NAME.default,
  keySpacePassword:   String          = SparkCassWriteConf.SPARK_CASSANDRA_BULK_WRITE_KEYSTORE_PASSWORD.default,
  connectionHost:     String          = SparkCassWriteConf.SPARK_CASSANDRA_BULK_WRITE_CONNECTION_HOSTS.default,
  connectionsPort:    Int             = SparkCassWriteConf.SPARK_CASSANDRA_BULK_WRITE_CONNECTION_PORT.default,
  ttl:                TTLOption       = TTLOption.defaultValue,
  timestamp:          TimestampOption = TimestampOption.defaultValue) extends Serializable {
  require(
    SparkCassWriteConf.AllowedPartitioners.contains(partitioner),
    s"Invalid value of spark.cassandra.bulk.write.partitioner: $partitioner. " +
      s"Expected any of ${SparkCassWriteConf.AllowedPartitioners.mkString(", ")}.")

  private[cassandra] def getIPartitioner: IPartitioner = {
    partitioner match {
      case "org.apache.cassandra.dht.Murmur3Partitioner" =>
        new Murmur3Partitioner()
      case "org.apache.cassandra.dht.RandomPartitioner" =>
        new RandomPartitioner()
      case "org.apache.cassandra.dht.ByteOrderedPartitioner" =>
        new ByteOrderedPartitioner()
    }
  }

  private[cassandra] val optionPlaceholders: Seq[String] =
    Seq(ttl, timestamp).collect {
      case WriteOption(PerRowWriteOptionValue(placeholder)) => placeholder
    }

  private[cassandra] val optionsAsColumns: (String, String) => Seq[ColumnDef] = { (keyspace, table) =>
    def toRegularColDef(opt: WriteOption[_], dataType: DataType) = opt match {
      case WriteOption(PerRowWriteOptionValue(placeholder)) =>
        Some(
          ColumnDef(
            placeholder,
            RegularColumn,
            ColumnType.fromDriverType(dataType)))
      case _ => None
    }

    Seq(
      toRegularColDef(ttl, DataType.cint()),
      toRegularColDef(timestamp, DataType.bigint())).flatten
  }

  val throttlingEnabled = throughputMiBPS < SparkCassWriteConf.SPARK_CASSANDRA_BULK_WRITE_THROUGHPUT_MB_PER_SEC.default
}

object SparkCassWriteConf {
  val AllowedPartitioners = Set(
    "org.apache.cassandra.dht.Murmur3Partitioner",
    "org.apache.cassandra.dht.RandomPartitioner",
    "org.apache.cassandra.dht.ByteOrderedPartitioner")

  val SPARK_CASSANDRA_BULK_WRITE_PARTITIONER = SparkCassConfParam[String](
    name = "spark.cassandra.bulk.write.partitioner",
    default = "org.apache.cassandra.dht.Murmur3Partitioner")

  val SPARK_CASSANDRA_BULK_WRITE_THROUGHPUT_MB_PER_SEC = SparkCassConfParam[Int](
    name = "spark.cassandra.bulk.write.throughput_mb_per_sec",
    default = Int.MaxValue)

  val SPARK_CASSANDRA_BULK_WRITE_CONNECTIONS_PER_HOST = SparkCassConfParam[Int](
    name = "spark.cassandra.bulk.write.connection_per_host",
    default = 1)

  val SPARK_CASSANDRA_BULK_WRITE_KEYSPACE_USER_NAME = SparkCassConfParam[String](
    name = "spark.cassandra.auth.username",
    default = "cassandra")

  val SPARK_CASSANDRA_BULK_WRITE_KEYSTORE_PASSWORD = SparkCassConfParam[String](
    name = "spark.cassandra.auth.password",
    default = "cassandra")

  val SPARK_CASSANDRA_BULK_WRITE_CONNECTION_HOSTS = SparkCassConfParam[String](
    name = "spark.cassandra.connection.host",
    default = "127.0.0.1")

  val SPARK_CASSANDRA_BULK_WRITE_CONNECTION_PORT = SparkCassConfParam[Int](
    name = "spark.cassandra.connection.port",
    default = 9042)

  /**
   * Extracts [[SparkCassWriteConf]] from a [[SparkConf]].
   *
   * @param sparkConf A [[SparkConf]].
   * @return A [[SparkCassWriteConf]] from a [[SparkConf]].
   */
  def fromSparkConf(sparkConf: SparkConf): SparkCassWriteConf = {
    val tempPartitioner = sparkConf.get(
      SPARK_CASSANDRA_BULK_WRITE_PARTITIONER.name,
      SPARK_CASSANDRA_BULK_WRITE_PARTITIONER.default)
    val tempThroughputMiBPS = sparkConf.getInt(
      SPARK_CASSANDRA_BULK_WRITE_THROUGHPUT_MB_PER_SEC.name,
      SPARK_CASSANDRA_BULK_WRITE_THROUGHPUT_MB_PER_SEC.default)
    val tempConnectionsPerHost = sparkConf.getInt(
      SPARK_CASSANDRA_BULK_WRITE_CONNECTIONS_PER_HOST.name,
      SPARK_CASSANDRA_BULK_WRITE_CONNECTIONS_PER_HOST.default)
    val tempKeySpaceUserName = sparkConf.get(
      SPARK_CASSANDRA_BULK_WRITE_KEYSPACE_USER_NAME.name,
      SPARK_CASSANDRA_BULK_WRITE_KEYSPACE_USER_NAME.default)
    val tempKeySpacePassword = sparkConf.get(
      SPARK_CASSANDRA_BULK_WRITE_KEYSTORE_PASSWORD.name,
      SPARK_CASSANDRA_BULK_WRITE_KEYSTORE_PASSWORD.default)
    val tempConnectionHost = sparkConf.get(
      SPARK_CASSANDRA_BULK_WRITE_CONNECTION_HOSTS.name,
      SPARK_CASSANDRA_BULK_WRITE_CONNECTION_HOSTS.default)
    val tempConnectionsPort = sparkConf.getInt(
      SPARK_CASSANDRA_BULK_WRITE_CONNECTION_PORT.name,
      SPARK_CASSANDRA_BULK_WRITE_CONNECTION_PORT.default)

    require(
      AllowedPartitioners.contains(tempPartitioner),
      s"Invalid value of spark.cassandra.bulk.write.partitioner: $tempPartitioner. " +
        s"Expected any of ${AllowedPartitioners.mkString(", ")}.")

    SparkCassWriteConf(
      partitioner = tempPartitioner,
      throughputMiBPS = tempThroughputMiBPS,
      connectionsPerHost = tempConnectionsPerHost,
      keySpaceUserName = tempKeySpaceUserName,
      keySpacePassword = tempKeySpacePassword,
      connectionHost = tempConnectionHost,
      connectionsPort = tempConnectionsPort)
  }
}
