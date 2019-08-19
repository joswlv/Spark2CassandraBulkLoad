package com.github.joswlv.spark.cassandra.bulk

import java.io.File
import java.net.InetAddress
import java.util.UUID

import com.datastax.driver.core.PreparedStatement
import com.datastax.spark.connector.cql.{ CassandraConnector, ColumnDef, Schema, TableDef }
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.CountingIterator
import com.datastax.spark.connector.util.Quote.quote
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector.{ CollectionColumnName, ColumnRef, ColumnSelector }
import com.github.joswlv.spark.cassandra.bulk.conf.SparkCassWriteConf
import com.github.joswlv.spark.cassandra.bulk.util.SparkCassException
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.hadoop.cql3.CqlBulkRecordWriter
import org.apache.cassandra.io.sstable.{ CQLSSTableWriter, SSTableLoader }
import org.apache.cassandra.utils.OutputHandler
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

class SparkCassandraBulkWriter[T](
  cassandraConnector: CassandraConnector,
  tableDef:           TableDef,
  columnSelector:     IndexedSeq[ColumnRef],
  rowWriter:          RowWriter[T],
  sparkCassWriteConf: SparkCassWriteConf) extends Serializable with Logging {

  val keyspaceName: String = tableDef.keyspaceName
  val tableName: String = tableDef.tableName
  val columnNames: Seq[String] = rowWriter.columnNames diff sparkCassWriteConf.optionPlaceholders
  val columns: Seq[ColumnDef] = columnNames.map(tableDef.columnByName)

  val defaultTTL: Option[Long] = sparkCassWriteConf.ttl match {
    case TTLOption(StaticWriteOptionValue(value)) => Some(value)
    case _                                        => None
  }

  val defaultTimestamp: Option[Long] = sparkCassWriteConf.timestamp match {
    case TimestampOption(StaticWriteOptionValue(value)) => Some(value)
    case _ => None
  }

  private def initializeSchemaTemplate(): String = {
    tableDef.cql
  }

  private def initializeInsertTemplate(): String = {
    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(":" + _).mkString(", ")

    val ttlSpec = sparkCassWriteConf.ttl match {
      case TTLOption(PerRowWriteOptionValue(placeholder)) => Some(s"TTL :$placeholder")
      case TTLOption(StaticWriteOptionValue(value)) => Some(s"TTL $value")
      case _ => None
    }

    val timestampSpec = sparkCassWriteConf.timestamp match {
      case TimestampOption(PerRowWriteOptionValue(placeholder)) => Some(s"TIMESTAMP :$placeholder")
      case TimestampOption(StaticWriteOptionValue(value)) => Some(s"TIMESTAMP $value")
      case _ => None
    }

    val options = List(ttlSpec, timestampSpec).flatten
    val optionsSpec = if (options.nonEmpty) s"USING ${options.mkString(" AND ")}" else ""

    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec) $optionsSpec".trim
  }

  private[cassandra] val schemaTemplate: String = initializeSchemaTemplate()

  private[cassandra] val insertTemplate: String = initializeInsertTemplate()

  private[cassandra] def externalClientConfig: Configuration = {
    val conf = new Configuration()
    conf.set("cassandra.output.thrift.address", sparkCassWriteConf.connectionHost)
    conf.set("cassandra.output.native.port", "" + sparkCassWriteConf.connectionsPort)
    conf.set("cassandra.output.keyspace.username", sparkCassWriteConf.keySpaceUserName)
    conf.set("cassandra.output.keyspace.passwd", sparkCassWriteConf.keySpacePassword)
    conf
  }

  private[cassandra] def prepareSSTableDirectory(): File = {
    val temporaryRoot = System.getProperty("java.io.tmpdir")

    val maxAttempts = 10
    var currentAttempts = 0

    var ssTableDirectory: Option[File] = None
    while (ssTableDirectory.isEmpty) {
      currentAttempts += 1
      if (currentAttempts > maxAttempts) {
        throw new SparkCassException(
          s"Failed to create a SSTable directory of $keyspaceName.$tableName after $maxAttempts attempts!")
      }

      try {
        ssTableDirectory = Some {
          val newSSTablePath = s"spark-${UUID.randomUUID.toString}" +
            s"${File.separator}$keyspaceName${File.separator}$tableName"

          val tempFile = new File(temporaryRoot, newSSTablePath)
          tempFile.deleteOnExit()
          tempFile
        }
        if (ssTableDirectory.get.exists() || !ssTableDirectory.get.mkdirs()) {
          ssTableDirectory = None
        }
      } catch {
        case e: SecurityException => ssTableDirectory = None
      }
    }

    ssTableDirectory.get.getCanonicalFile
  }

  private[cassandra] def prepareDataStatement(): PreparedStatement = {
    try {
      cassandraConnector.openSession().prepare(insertTemplate)
    } catch {
      case t: Throwable =>
        throw new SparkCassException(s"Failed to prepare insert statement $insertTemplate: ${t.getMessage}", t)
    }
  }

  private[cassandra] def writeRowsToSSTables(
    ssTableDirectory: File,
    statement:        PreparedStatement,
    data:             Iterator[T]): Unit = {
    val ssTableBuilder = CQLSSTableWriter.builder()
      .inDirectory(ssTableDirectory)
      .forTable(schemaTemplate)
      .using(insertTemplate)
      .withPartitioner(sparkCassWriteConf.getIPartitioner)
    val ssTableWriter = ssTableBuilder.build()

    logInfo(s"Writing rows to temporary SSTables in ${ssTableDirectory.getAbsolutePath}.")

    val startTime = System.nanoTime()

    val rowIterator = new CountingIterator(data)
    val rowColumnNames = rowWriter.columnNames.toIndexedSeq
    val rowColumnTypes = rowColumnNames.map(statement.getVariables.getType)
    val rowConverters = rowColumnTypes.map(ColumnType.converterToCassandra)
    val rowBuffer = Array.ofDim[Any](columnNames.size)
    for (currentData <- rowIterator) {
      rowWriter.readColumnValues(currentData, rowBuffer)

      val rowValues = for (index <- columnNames.indices) yield {
        val currentConverter = rowConverters(index)
        val currentValue = currentConverter.convert(rowBuffer(index))

        currentValue
      }

      ssTableWriter.addRow(rowValues: _*)
    }

    ssTableWriter.close()

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1000000000d

    logInfo(s"Wrote rows to temporary SSTables in ${ssTableDirectory.getAbsolutePath} in $duration%.3f s.")
  }

  private[cassandra] def streamSSTables(ssTableDirectory: File): Unit = {
    val currentConnectionsPerHost = sparkCassWriteConf.connectionsPerHost
    val currentStreamEventHandler = new SparkCassStreamEventHandler(log)

    val externalClient = new CqlBulkRecordWriter.ExternalClient(externalClientConfig)

    val ssTableLoader = new SSTableLoader(ssTableDirectory, externalClient, new OutputHandler.LogOutput, currentConnectionsPerHost)

    if (sparkCassWriteConf.throttlingEnabled) {
      DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(sparkCassWriteConf.throughputMiBPS)
    }

    try {
      // TODO: Investigate whether sparkCassWriteConf should have a blacklist for InetAddresses to ignore.
      ssTableLoader.stream(Set.empty[InetAddress].asJava, currentStreamEventHandler).get()
    } catch {
      case e: Exception =>
        throw new SparkCassException(s"Failed to write statements to $keyspaceName.$tableName.", e)
    }
  }

  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {
    val tempSSTableDirectory = prepareSSTableDirectory()

    logInfo(s"Created temporary file directory for SSTables at ${tempSSTableDirectory.getAbsolutePath}.")

    try {
      val ssTableStatement = prepareDataStatement()

      writeRowsToSSTables(tempSSTableDirectory, ssTableStatement, data)

      streamSSTables(tempSSTableDirectory)

      logInfo(s"Finished stream of SSTables from ${tempSSTableDirectory.getAbsolutePath}.")
    } finally {
      // retry delete files when file was not unlocked another threads yet
      var loop = 5;
      while (loop > 0) {
        if (tempSSTableDirectory.exists()) {
          try {
            FileUtils.deleteDirectory(tempSSTableDirectory)
            loop = 0
          } catch {
            case e: Exception => {
              logInfo(s"file delete exception $e")
              Thread.sleep(1000)
              loop = loop - 1
            }
          }
        }
      }
      logInfo("tmp delete complete")
    }
  }
}

object SparkCassandraBulkWriter {

  private[cassandra] def checkMissingColumns(table: TableDef, columnNames: Seq[String]) {
    val allColumnNames = table.columns.map(_.columnName)
    val missingColumns = columnNames.toSet -- allColumnNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Column(s) not found: ${missingColumns.mkString(", ")}")
  }

  private[cassandra] def checkMissingPrimaryKeyColumns(table: TableDef, columnNames: Seq[String]) {
    val primaryKeyColumnNames = table.primaryKey.map(_.columnName)
    val missingPrimaryKeyColumns = primaryKeyColumnNames.toSet -- columnNames
    if (missingPrimaryKeyColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Some primary key columns are missing in RDD " +
          s"or have not been selected: ${missingPrimaryKeyColumns.mkString(", ")}")
  }

  private[cassandra] def checkNoCollectionBehaviors(table: TableDef, columnRefs: IndexedSeq[ColumnRef]) {
    if (columnRefs.exists(_.isInstanceOf[CollectionColumnName]))
      throw new IllegalArgumentException(
        s"Collection behaviors (add/remove/append/prepend) are not allowed on collection columns")
  }

  private[cassandra] def checkColumns(table: TableDef, columnRefs: IndexedSeq[ColumnRef]) = {
    val columnNames = columnRefs.map(_.columnName)
    checkMissingColumns(table, columnNames)
    checkMissingPrimaryKeyColumns(table, columnNames)
    checkNoCollectionBehaviors(table, columnRefs)
  }

  def apply[T: RowWriterFactory](
    connector:          CassandraConnector,
    keyspaceName:       String,
    tableName:          String,
    columnNames:        ColumnSelector,
    sparkCassWriteConf: SparkCassWriteConf): SparkCassandraBulkWriter[T] = {
    val schema = Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName))
    val tableDef = schema.tables.headOption
      .getOrElse(throw new SparkCassException(s"Table not found: $keyspaceName.$tableName"))
    val selectedColumns = columnNames.selectFrom(tableDef)
    val optionColumns = sparkCassWriteConf.optionsAsColumns(keyspaceName, tableName)
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
      tableDef.copy(regularColumns = tableDef.regularColumns ++ optionColumns),
      selectedColumns ++ optionColumns.map(_.ref))

    checkColumns(tableDef, selectedColumns)
    new SparkCassandraBulkWriter[T](connector, tableDef, selectedColumns, rowWriter, sparkCassWriteConf)
  }
}
