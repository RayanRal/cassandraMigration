package com.gmail.cassandra.migration

import com.gmail.cassandra.migration.data.{Connector, JsonOperations}

/**
 * @author rayanral
 */
object DbCopyTool extends JsonOperations {

  def main(args: Array[String]) {
    val con = new Connector

    val config = Option(System.getProperty("dbschema")).getOrElse(s"dbschema_${args.head}_${args(2)}.json")
    val (columns, blobColumns) = parseJson(config)
    args.toList match {
      case "export" :: t =>
        con.copyFrom(t(0), t(1), columns, blobColumns)
      case "import" :: t =>
        con.copyTo(t(0), t(1), columns, blobColumns)
    }
    con.close()
  }

}
