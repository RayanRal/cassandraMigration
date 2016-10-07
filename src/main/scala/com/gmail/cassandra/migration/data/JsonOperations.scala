package com.gmail.cassandra.migration.data

import spray.json._

import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * @author rayanral on 10/09/15.
 */
trait JsonOperations {

  def convertToJson(result: String, errorMsg: String = "Cannot parse: "): JsValue = {
    Try(result.parseJson) match {
      case s: Success[JsValue] =>
        s.get
      case e: Failure[JsValue] =>
        throw new IllegalArgumentException(errorMsg + e)
    }
  }

  def parseJson(filename: String) = {
    val migrationConfig = convertToJson(Source.fromFile(filename).mkString).asJsObject.fields.toMap

    val columns = migrationConfig.getOrElse[JsValue]("columns", JsArray()) match {
      case JsArray(elements) => elements.map(v => v.toString()).toList.map(s => s.init.tail)
      case _ => sys.error("Oops! JsArray expected.")
    }
    val blobColumns = migrationConfig.getOrElse[JsValue]("blobColumns", JsArray()) match {
      case JsArray(elements) => elements.map(v => v.toString()).toList.map(s => s.init.tail)
      case _ => sys.error("Oops! JsArray expected.")
    }

    (columns, blobColumns)
  }

}
