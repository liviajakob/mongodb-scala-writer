package com.earthwave.core

case class GridCell( x : Long, y : Long, size : Long )
case class ShardDetail( dsName : String, shardName : String, gridCellMinX : Long, gridCellMaxX : Long, gridCellMinY : Long, gridCellMaxY : Long, gridCellSize : Long, minX : Long, maxX : Long, minY : Long, maxY : Long, minTime : Long, maxTime : Long, count : Long )

