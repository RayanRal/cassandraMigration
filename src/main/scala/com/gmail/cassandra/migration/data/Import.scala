package com.gmail.cassandra.migration.data

import java.io._

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.querybuilder.QueryBuilder

/**
 * @author rayanral
 */
trait Import extends CsvOperations with DbOperations {
  this: CassandraInit =>

  def copyTo(keyspace: String, table: String, columns: List[String], blobColumns: List[String]) = {
    import com.datastax.driver.core.DataType.Name._

    val values = readCsvFile(table)
    val cols = getColumnsMetadata(keyspace, table, columns)
    val pks = getPrimaryKey(keyspace, table)

    values.foreach { l =>
      val valuesAndCols = l.zip(cols)
      val primaryKeyValues = valuesAndCols.map {
        case (value, (colName, colType)) if pks.contains(colName) => (colName, value)
        case (value, (colName, colType)) => (colName, "")
      }.filter(_._2.nonEmpty).sortBy(_._1).map(_._2).mkString("_")

      val parsedValues = valuesAndCols.map {
        case (value, (colName, colType)) if colType.getName == BLOB || blobColumns.contains(colName) =>
          (readBlob(table, colName, primaryKeyValues), (colName, colType))
        case ("<null>", (colName, colType)) =>
          (null, (colName, colType))
        case (value, (colName, colType)) if colType.getName == TEXT =>
          (colType.parse(s"'$value'"), (colName, colType))
        case (value, (colName, colType)) =>
          (colType.parse(value), (colName, colType))
      }.unzip

      globalSession.execute(QueryBuilder.insertInto(keyspace, table).values(cols.unzip._1.toArray, parsedValues._1.toArray).setConsistencyLevel(ConsistencyLevel.QUORUM))
    }
  }

  def readBlob(table: String, colName: String, foldername: String) = {
    val file = new File(s"$table/$foldername/$colName")
    val fileData = new Array[Byte](file.length().toInt)
    val dis = new DataInputStream(new FileInputStream(file))
    dis.readFully(fileData)
    dis.close()
    new String(fileData.map(_.toChar))
  }

}
