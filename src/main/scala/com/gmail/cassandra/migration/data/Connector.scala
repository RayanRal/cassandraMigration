package com.gmail.cassandra.migration.data

import com.datastax.driver.core.Cluster
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._
import scala.reflect.io.Path
import scala.util.Try


trait CassandraInit {
  val configLocation = Option(System.getProperty("config")).getOrElse("dev-server.conf")
  val thisConfig = ConfigFactory.parseFile(Path(configLocation).jfile).getConfig("main.db.cassandra")
  val keyspaceName = thisConfig.getString("keyspace")
  val port = thisConfig.getInt("port")
  val hosts = thisConfig.getStringList("hosts").asScala.toList
  val fetchSize = Try(thisConfig.getInt("fetchSize")).getOrElse(50)
  val fileName = Option(System.getProperty("filename")).getOrElse("data.csv")

  val cluster = Cluster.builder().addContactPoints(hosts: _*).withPort(port).build()
  val globalSession = cluster.connect(keyspaceName)
}

class Connector extends CassandraInit with Export with Import {
  def close() = {
    globalSession.closeAsync()
    cluster.closeAsync()
  }

}