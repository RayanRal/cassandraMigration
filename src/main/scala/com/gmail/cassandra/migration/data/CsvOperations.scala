package com.gmail.cassandra.migration.data

import java.io.File

import com.github.tototoshi.csv._

import scala.collection.mutable.ListBuffer

/**
 * @author rayanral on 08/09/15.
 */
trait CsvOperations {
  this: CassandraInit =>

  def writeCsvFile(data: List[List[Option[String]]], tablename: String, foldername: String) = {
    val dir = new File(s"$tablename/$foldername").mkdirs()
    val f = new File(s"$tablename/$foldername/$fileName.csv")
    val writer = CSVWriter.open(f)(CustomFormat)
    writer.writeAll(data.map(_.map(o => o.getOrElse("<null>"))))
    writer.close()
  }

  def writeAllToOneCsvFile(data: List[List[Option[String]]], tablename: String) = {
    val dir = new File(s"$tablename/").mkdirs()
    val f = new File(s"$tablename/$fileName.csv")
    val writer = CSVWriter.open(f)(CustomFormat)
    writer.writeAll(data.map(_.map(o => o.getOrElse("<null>"))))
    writer.close()
  }

  def readCsvFile(foldername: String): List[List[String]] = {
    val reader = CSVReader.open(new File(s"$foldername/$fileName.csv"))(CustomFormat)
    val result = reader.all()
    reader.close()
    result
  }

  def getAllCsvFilesFromFolder(foldername: String): List[List[String]] = {
    val dirsFound = new File(s"$foldername") match {
      case dirFound if dirFound.exists() => dirFound.listFiles().filter(_.isDirectory)
      case dirNotFound => sys.error(s"Import directory: $foldername - not found")
    }
    val res: ListBuffer[List[List[String]]] = new ListBuffer[List[List[String]]]()
    dirsFound.foreach { dirname =>
      val reader = CSVReader.open(new File(s"$dirname/$fileName.csv"))(CustomFormat)
      res.append(reader.all())
      reader.close()
    }
    res.toList.flatten
  }


  object CustomFormat extends DefaultCSVFormat {
    override val delimiter = ';'
  }

}
