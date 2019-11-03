package com.flink.examples

import com.flink.sinks.WordCountCassandraSink
import com.flink.sources.WordCountSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object CassandraWordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val wordCountSource: DataStream[(String, Long)] = env.addSource(new WordCountSource())

    wordCountSource.print()

    WordCountCassandraSink.sinkToCassandra(wordCountSource)

    env.execute("Cassandra Word Count Example")
  }
}
