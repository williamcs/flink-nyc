package com.flink.sources

import com.flink.utils.WordCountData
import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * Word count source from some test words data.
  */
class WordCountSource extends SourceFunction[(String, Long)] {

  override def run(ctx: SourceFunction.SourceContext[(String, Long)]): Unit = {
    val text = WordCountData.WORDS
    val count: Map[String, Long] = text
      .flatMap(_.split("\\W+").filter(_.nonEmpty))
      .map((_, 1L))
      .groupBy(_._1)
      .mapValues(_.length.toLong)

    // map Map[String, Long] to tuple (word, count)
    val wcTuple = count.map(wc => (wc._1, wc._2))

    // collect all word count tuple
    for (wc <- wcTuple) {
      ctx.collect(wc)
    }
  }

  override def cancel(): Unit = {
    println("read word count data is canceled!")
  }
}