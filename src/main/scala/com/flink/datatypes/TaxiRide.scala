package com.flink.datatypes

import java.util.Locale

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * A TaxiRide describes a taxi ride event.
  *
  * @param rideId
  * @param time
  * @param isStart
  * @param location
  * @param passengerCnt
  * @param travelDist
  */
class TaxiRide(
                var rideId: Long,
                var time: DateTime,
                var isStart: Boolean,
                var location: GeoPoint,
                var passengerCnt: Short,
                var travelDist: Float) {

  def this() {
    this(0, new DateTime(0), false, GeoPoint(0.0, 0.0), 0, 0.0f)
  }

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder

    sb.append(rideId).append(",")
    sb.append(time.toString(TaxiRide.TimeFormatter)).append(",")
    sb.append(if (isStart) "START" else "END").append(",")
    sb.append(location.lon).append(",")
    sb.append(location.lat).append(",")
    sb.append(passengerCnt).append(",")
    sb.append(travelDist)

    sb.toString()
  }
}

object TaxiRide {

  @transient
  private final val TimeFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-DD HH:mm:ss").withLocale(Locale.US).withZoneUTC()

  def fromString(line: String): TaxiRide = {

    val tokens: Array[String] = line.split(",")
    if (tokens.length != 7) {
      throw new RuntimeException("Invalid record: " + line)
    }

    try {
      val rideId = tokens(0).toLong
      val time = DateTime.parse(tokens(1), TimeFormatter)
      val isStart = tokens(2) == "START"
      val lon = if (tokens(3).length > 0) tokens(3).toDouble else 0.0
      val lat = if (tokens(4).length > 0) tokens(4).toDouble else 0.0
      val passengerCnt = tokens(5).toShort
      val travelDistance = if (tokens(6).length > 0) tokens(6).toFloat else 0.0f

      new TaxiRide(rideId, time, isStart, GeoPoint(lon, lat), passengerCnt, travelDistance)
    } catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid record: " + line, nfe)
    }
  }
}

/**
  * A geo point defined by a longitude and latitude value.
  *
  * @param lon
  * @param lat
  */
case class GeoPoint(lon: Double, lat: Double)
