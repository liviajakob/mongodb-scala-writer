package com.earthwave.core

import java.lang

import akka.actor.{ActorLogging, ActorRef}
import com.stream_financial.publib.connector.ConnectorSession
import com.stream_financial.publib.table.DFTable

case class GridCell( x : Long, y : Long, size : Long )
case class ShardDetail( dsName : String, shardName : String, swathName : String, gridCellMinX : Long, gridCellMaxX : Long, gridCellMinY : Long, gridCellMaxY : Long, gridCellSize : Long, minX : Long, maxX : Long, minY : Long, maxY : Long, minTime : Long, maxTime : Long, count : Long )

object ShardBuilder {


  def createShards( shard : (FileHeader,Map[GridCell,Vector[Vector[java.lang.Double]]]), gridCellSize : Long, log : ActorLogging, cat: ActorRef) ={

    def addColumn( table : DFTable, columnName :String, data : Vector[java.lang.Double], longCols : Set[String] ):Boolean ={

      if( longCols.contains(columnName) )
      {
        table.add(DFHelper.createColumnLong(columnName, data.map(x => x.toLong))  )
        return true;
      }
      else
      {
        table.add(DFHelper.createColumnNullableDouble(columnName, data))
        return true;
      }
    }

    val longCols = Set("startTime","x","y")

    val header = shard._1
    val data = shard._2

    val session = new ConnectorSession(Constants.dfConnectorHost,Constants.dfConnectorPort, Constants.dataOutputPath)

    for( (k,v) <- shard._2 ) {
      log.log.info(s"Creating Shard for Grid Cell x=${k.x} y=${k.y} size=${k.size}")
      val table = new DFTable("Data")
      val res: (Vector[Vector[lang.Double]], Long) = Profile.profile(v.transpose)
      log.log.info(s"Transpose took : ${res._2} millis")

      val cols = res._1
      header.columns.foreach(col => addColumn(table, col._1, cols(col._2), longCols))

      val status = Profile.profile(session.uploadTable(table))

      val shardName = status._1.split(": ")(1)

      val t = Profile.profile{
      val xIndex = header.getIndex("x")
      val yIndex = header.getIndex("y")
      val timeIndex = header.getIndex("startTime")
      val xMin = cols(xIndex).min.toLong
      val xMax = cols(xIndex).max.toLong
      val yMin = cols(yIndex).min.toLong
      val yMax = cols(yIndex).max.toLong
      val count = cols(xIndex).length
      val minTime = cols(timeIndex).min.toLong
      val maxTime = cols(timeIndex).max.toLong
      val sDetail = ShardDetail("CryoSat", shardName, shard._1.fileName, k.x, k.x + gridCellSize, k.y, k.y + gridCellSize, gridCellSize, xMin, xMax, yMin, yMax, minTime, maxTime, count )
      // send the catalogue entry.
      cat ! sDetail
        log.log.info( s"Shard $shardName took ${status._2} millis to upload ${count} rows.")
      }

      log.log.info(s"Stats took ${t._2} millis")


    }

    session.close()
  }
}
