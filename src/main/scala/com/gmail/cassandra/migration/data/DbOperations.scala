package com.gmail.cassandra.migration.data

import com.datastax.driver.core.DataType
import scala.collection.JavaConverters._

/**
 * @author rayanral on 08/09/15.
 */
trait DbOperations {
  this: CassandraInit =>

  def getColumnsMetadata(keyspace: String, table: String, columns: List[String] = List()) = {
    val tableMetadata = cluster.getMetadata.getKeyspace(keyspace).getTable(table)
    if(columns.nonEmpty) {
      columns.map(c => c -> tableMetadata.getColumn(c).getType)
    } else
      tableMetadata.getColumns.asScala.toList.map(col => col.getName -> col.getType)
  }

  def getPrimaryKey(keyspace: String, table: String): List[String] =
    cluster.getMetadata.getKeyspace(keyspace).getTable(table).getPrimaryKey.asScala.map(meta => meta.getName).toList

}
