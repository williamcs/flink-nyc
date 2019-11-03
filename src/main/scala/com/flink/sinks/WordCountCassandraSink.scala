package com.flink.sinks

import com.datastax.driver.core.{Cluster, Session}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}

object WordCountCassandraSink {

  private val HOST = "127.0.0.1"

  private val CREATE_KEYSPACE =
    """
      |CREATE KEYSPACE IF NOT EXISTS example
      |WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """.stripMargin

  private val CREATE_TABLE =
    """
      |CREATE TABLE IF NOT EXISTS example.wordcount (
      |    word text,
      |    count bigint,
      |    PRIMARY KEY(word)
      |    );
    """.stripMargin

  private val INSERT_QUERY =
    """
      |INSERT INTO example.wordcount(word, count) values (?, ?);
    """.stripMargin

  private val builder = new ClusterBuilder {
    override def buildCluster(builder: Cluster.Builder): Cluster = {
      builder.addContactPoint(HOST).build()
    }
  }

  /**
    * Create keyspace and table before insert the data.
    */
  def createKeySpaceAndTable(): Unit = {
    var cluster: Cluster = null
    var session: Session = null

    try {
      cluster = builder.getCluster
      session = cluster.connect

      session.execute(CREATE_KEYSPACE)
      session.execute(CREATE_TABLE)

    } catch {
      case e: Exception =>
        println("exception is: " + e.getCause)
    } finally {
      closeCassandra(cluster, session)
    }

  }

  /**
    * Sink to cassandra.
    *
    * @param wordCountSource
    */
  def sinkToCassandra(wordCountSource: DataStream[(String, Long)]): Unit = {
    createKeySpaceAndTable()

    CassandraSink
      .addSink(wordCountSource)
      .setQuery(INSERT_QUERY)
      .setHost(HOST)
      .build()
  }

  /**
    * Close cassandra.
    *
    * @param cluster
    * @param session
    */
  def closeCassandra(cluster: Cluster, session: Session): Unit = {
    if (session != null) {
      session.close()
    }

    if (cluster != null) {
      cluster.close()
    }
  }

}
