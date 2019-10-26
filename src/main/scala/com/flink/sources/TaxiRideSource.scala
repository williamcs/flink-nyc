package com.flink.sources

import java.io.{BufferedReader, FileInputStream, InputStream, InputStreamReader}
import java.util.zip.GZIPInputStream

import com.flink.datatypes.TaxiRide
import org.apache.flink.streaming.api.functions.source.SourceFunction

class TaxiRideSource(dataFilePath: String, maxDelaySecs: Int, servingSpeed: Float) extends SourceFunction[TaxiRide] {

  private val maxDelayMsecs = maxDelaySecs * 1000
  private val watermarkDelayMSecs = if (maxDelayMsecs < 10000) 10000 else maxDelayMsecs

  @transient
  private var reader: BufferedReader = null
  @transient
  private var gzipStream: InputStream = null


  override def run(ctx: SourceFunction.SourceContext[TaxiRide]): Unit = {
    gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath))
    reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"))

    if (this.maxDelayMsecs == 0) {

    } else {

    }

    this.reader.close()
    this.reader = null
    this.gzipStream.close()
    this.gzipStream = null
  }

  override def cancel(): Unit = ???
}
