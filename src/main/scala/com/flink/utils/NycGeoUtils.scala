package com.flink.utils

import com.flink.datatypes.GeoPoint

/**
  * GeoUtils provides utility methods to deal with GeoPoints with locations in New York City.
  */
object NycGeoUtils {

  val LonEast: Double = -73.7
  val LonWest: Double = -74.05
  val LatNorth: Double = 41.0
  val LatSouth: Double = 40.5

  val LonWidth: Double = 74.05 - 73.7
  val LatHeight: Double = 41.0 - 40.5

  val DeltaLon: Double = 0.0014
  val DeltaLat: Double = 0.00125

  val CellCntX: Int = 250
  val CellCntY: Int = 400

  /**
    * Checks if a location specified by longitude and latitude values is
    * within the geo boundaries of New York City.
    *
    * @param point
    * @return
    */
  def isInNYC(point: GeoPoint): Boolean = {
    if (point.lon > LonEast || point.lon < LonWest)
      false
    if (point.lat > LatNorth || point.lat < LatSouth)
      false

    true
  }

  /**
    * Maps a location specified as GeoPoint to a cell of a grid covering the area of NYC.
    * The grid cells are roughly 100 x 100 m and sequentially number from north-west
    * to south-east starting by zero.
    *
    * @param point
    * @return
    */
  def mapToGridCell(point: GeoPoint): Int = {
    val xIndex = Math.floor((Math.abs(LonWest) - Math.abs(point.lon)) / DeltaLon).toInt
    val yIndex = Math.floor((LatNorth - point.lat) / DeltaLat).toInt

    xIndex + (yIndex * CellCntX)
  }

  /**
    * Returns the center of a grid cell as a GeoPoint
    *
    * @param gridCellId
    * @return
    */
  def getGridCellCenter(gridCellId: Int): GeoPoint = {
    val xIndex: Int = gridCellId % CellCntX
    val lon = (Math.abs(LonWest) - (xIndex * DeltaLon) - (DeltaLon / 2)).toFloat * -1.0f

    val yIndex: Int = (gridCellId - xIndex) / CellCntX
    val lat = (LatNorth - (yIndex * DeltaLat) - (DeltaLat / 2)).toFloat

    new GeoPoint(lon, lat)
  }
}
