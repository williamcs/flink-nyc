package com.flink.examples

import com.flink.datatypes.{GeoPoint, TaxiRide}
import com.flink.sinks.CountByLocationCassandraSink
import com.flink.sources.TaxiRideSource
import com.flink.utils.NycGeoUtils
import com.google.gson.Gson
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * https://github.com/ANierbeck/BusFloatingData/blob/master/flink-digest/src/main/scala/de/nierbeck/floating/data/stream/flink/KafkaToCassandraFlinkApp.scala
  * https://github.com/scylladb/scylla-code-samples/blob/master/flink_streaming_into_scylla/flink-app/src/main/java/com/scylla/movies/FilterMoviesStreamingJob.java
  * https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#triggers
  * https://github.com/dataArtisans/flink-streaming-demo/blob/master/src/main/scala/com/dataartisans/flink_demo/examples/EarlyArrivalCount.scala
  */
object CountByLocation {

  def main(args: Array[String]): Unit = {

    // input parameters
    val data = "./data/nycTaxiData.gz"
    val maxServingDelay = 60
    val servingSpeedFactor = 600f

    // window parameters
    val countWindowLength = 15 // window size in min
    val countWindowFrequency = 5 // window trigger interval in min

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // define the data source
    val rides: DataStream[TaxiRide] = env.addSource(new TaxiRideSource(data, maxServingDelay, servingSpeedFactor))

    // transformations
    val cntByLocationJson: DataStream[(Int, Long, String, Int)] = transform(rides, countWindowLength, countWindowFrequency)

    cntByLocationJson.print()

    // sink to cassandra
    CountByLocationCassandraSink.sinkToCassandra(cntByLocationJson)

    env.execute("Early arrival counts per location")
  }

  def transform(rides: DataStream[TaxiRide], countWindowLength: Int, countWindowFrequency: Int): DataStream[(Int, Long, String, Int)] = {
    val cleansedRides = rides
      // filter for trip end events
      .filter(!_.isStart)
      // filter for events in NYC
      .filter(r => NycGeoUtils.isInNYC(r.location))

    val cellIds: DataStream[(Int, Short)] = cleansedRides.map(r => (NycGeoUtils.mapToGridCell(r.location), r.passengerCnt))

    val passengerCnts: DataStream[(Int, Long, Int)] = cellIds
      .keyBy(_._1)
      .timeWindow(Time.minutes(countWindowLength), Time.minutes(countWindowFrequency))
      .apply {
        (
          cell: Int,
          window: TimeWindow,
          events: Iterable[(Int, Short)],
          out: Collector[(Int, Long, Int)]) =>
          out.collect((cell, window.getEnd, events.map(_._2).sum))
      }

    //    passengerCnts.print()

    val cntByLocation: DataStream[(Int, Long, GeoPoint, Int)] = passengerCnts
      // map cell Id back to GeoPoint
      .map(r => (r._1, r._2, NycGeoUtils.getGridCellCenter(r._1), r._3))

    val cntByLocationJson: DataStream[(Int, Long, String, Int)] = cntByLocation.map(r => (r._1, r._2, new Gson().toJson(r._3), r._4))

    cntByLocationJson
  }

}
