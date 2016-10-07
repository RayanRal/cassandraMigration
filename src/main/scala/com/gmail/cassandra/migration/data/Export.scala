package com.gmail.cassandra.migration.data

import java.io.{PrintWriter, File, FileOutputStream}

import com.datastax.driver.core.DataType.Name._
import com.datastax.driver.core.querybuilder.{Select, QueryBuilder}
import com.datastax.driver.core.{Statement, ConsistencyLevel, DataType, Row}
import scala.collection.JavaConverters._

/**
 * @author rayanral on 08/09/15.
 */
trait Export extends CsvOperations with DbOperations {
  this: CassandraInit =>

  def copyFrom(keyspace: String, table: String, columns: List[String], blobColumns: List[String]) = {

    def createWheresForQuery(pks: List[String]) = {
      val pkValuesRows = globalSession.execute(QueryBuilder.select(pks: _*).from(keyspace, table).setFetchSize(fetchSize)).iterator().asScala
      val pkRowsForQuery = pkValuesRows.map { pkRow => pks.map { primKey => primKey -> extractRowByType[Any](primKey, pkRow, a => a).get }}.toList
      val clausesList = pkRowsForQuery.map { pkRow => pkRow.map(elem => QueryBuilder.eq(elem._1, elem._2)) }

      val wheresList: List[Select.Where] = for {
        clauses <- clausesList
        where = if(columns.nonEmpty) QueryBuilder.select(columns: _*).from(keyspace, table).where() else QueryBuilder.select.all().from(keyspace, table).where()
        clause <- clauses
        lastWhere = where.and(clause)
      } yield where
      wheresList.toSet.toList.map((w: Select.Where) => w.setConsistencyLevel(ConsistencyLevel.QUORUM))
    }

    val columnsData = getColumnsMetadata(keyspace, table, columns)
    val pks = getPrimaryKey(keyspace, table)
    val wheresList = createWheresForQuery(pks)

    val rs = for {
      oneWhere <- wheresList
      row <- globalSession.execute(oneWhere).iterator().asScala
      parsedRow = parseRow(row, columnsData, blobColumns, pks, table)
    } yield parsedRow
    writeAllToOneCsvFile(rs, table)
  }

  def parseRow(row: Row, columns: List[(String, DataType)], blobColumns: List[String], pks: List[String], tablename: String, separator: String = ";") = {
    val primaryKey = pks.sorted.map{s =>
      extractRowByType[String](s, row, _.toString).orNull
    }.mkString("_")

    val parsedRow = columns.map { column =>
      import com.datastax.driver.core.DataType.Name._

      if(blobColumns.contains(column._1) && column._2.getName != BLOB) {
        saveBlobFromText(row, column._1, tablename, primaryKey)
        Some("")
      } else {
        extractRowByType[String](column._1, row, _.toString, tablename, primaryKey)
      }
    }.toList

    parsedRow
  }

  def extractRowByType[T](columnName: String, row: Row, mapper: Any => T, tablename: String = "", primaryKey: String = ""): Option[T] = {
    val colValue = row.getColumnDefinitions.getType(columnName).getName match {
      case TIMESTAMP =>
        Option(row.getDate(columnName)).map(_.getTime)
      case BOOLEAN =>
        Option(row.getBool(columnName))
      case UUID | TIMEUUID =>
        Option(row.getUUID(columnName))
      case VARINT =>
        Option(row.getVarint(columnName))
      case DOUBLE =>
        Option(row.getDouble(columnName))
      case INT =>
        Option(row.getInt(columnName))
      case BIGINT | COUNTER =>
        Option(row.getLong(columnName))
      case DECIMAL =>
        Option(row.getDecimal(columnName))
      case FLOAT =>
        Option(row.getFloat(columnName))
      case INET =>
        Option(row.getInet(columnName))
      case BLOB =>
        saveBlob(row, columnName, tablename, primaryKey)
        Some("")
      case other =>
        Option(row.getString(columnName))
    }
    colValue.map(mapper)
  }

  def saveBlobFromText(row: Row, columnName: String, tablename: String, foldername: String) = {
    val bytes = row.getString(columnName).toCharArray.map(_.toByte)
    new File(s"$tablename/$foldername").mkdirs() match {
      case true =>
        val fos = new FileOutputStream(new File(s"$tablename/$foldername/$columnName"))
        fos.write(bytes)
        fos.close()
      case false =>
        sys.error(s"Could not create directory $tablename/$foldername, export stopped")
    }
  }

  def saveBlob(row: Row, columnName: String, tablename: String, foldername: String) = {
    val bb = row.getBytesUnsafe(columnName)
    val dir = new File(s"$tablename/$foldername").mkdirs() match {
      case true =>
        val fos = new FileOutputStream(new File(s"$tablename/$foldername/$columnName")).getChannel
        fos.write(bb)
        fos.close()
      case false =>
        sys.error(s"Could not create directory $tablename/$foldername, export stopped")
    }
  }

}
