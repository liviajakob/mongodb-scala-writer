package com.earthwave.core

import java.lang

import com.stream_financial.publib.connector.ConnectorSession
import com.stream_financial.publib.table.DFTable

case class GridCell( x : Long, y : Long, size : Long )

case class ShardDetail( name : String, minX : Long, maxX : Long, minY : Long, maxY : Long, minTime : Long, maxTime : Long, count : Long )

object DataSetLoader {


  def addColumn( table : DFTable, columnName :String, data : Vector[lang.Double], longCols : Set[String] ):Boolean ={

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

  def createShards( shard : (FileHeader,Map[GridCell,Vector[Vector[lang.Double]]]), gridCellSize : Long ) ={

    val longCols = Set("startTime","x","y")

    val header = shard._1
    val data = shard._2

    val session = new ConnectorSession("localhost",9001, "EarthWave")

    var shardDetails = Vector[(String, ShardDetail)]()

    for( (k,v) <- shard._2 ) {
      println(s"Creating Shard for Grid Cell x=${k.x} y=${k.y} size=${k.size}")
      val table = new DFTable("Data")
      val cols = v.transpose
      header.columns.foreach(col => addColumn(table, col._1, cols(col._2), longCols))

      val status = session.uploadTable(table)

      val shardName = status.split(": ")(1)

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

      val sDetail = ShardDetail(shardName, xMin, xMax, yMin, yMax, minTime, maxTime, count )

      shardDetails = shardDetails.:+((status,sDetail))
      println(s"$status")
    }

    val catalogue = new DFTable("Catalogue" )
    val numberShards = data.keys.toList.length
    val cells = data.keys.toList

    catalogue.add( DFHelper.createColumnString("Name", Vector.fill(numberShards)("CryoSat")))
    catalogue.add( DFHelper.createColumnString("CDBShardName", shardDetails.map(x => x._2.name)))
    catalogue.add( DFHelper.createColumnLong("GridCellMinX", cells.map(x => x.x ).toVector ) )
    catalogue.add( DFHelper.createColumnLong("GridCellMaxX", cells.map(x => x.x + gridCellSize ).toVector ) )
    catalogue.add( DFHelper.createColumnLong("GridCellMinY", cells.map(x => x.y ).toVector ) )
    catalogue.add( DFHelper.createColumnLong("GridCellMaxY", cells.map(x => x.y + gridCellSize ).toVector ) )
    catalogue.add( DFHelper.createColumnLong("GridCellSize", cells.map(s => s.size).toVector ) )
    catalogue.add( DFHelper.createColumnLong("ShardMinX", shardDetails.map(x => x._2.minX) ) )
    catalogue.add( DFHelper.createColumnLong("ShardMaxX", shardDetails.map(x => x._2.maxX) ) )
    catalogue.add( DFHelper.createColumnLong("ShardMinY", shardDetails.map(x => x._2.minY) ) )
    catalogue.add( DFHelper.createColumnLong("ShardMaxY", shardDetails.map(x => x._2.maxY) ) )
    catalogue.add( DFHelper.createColumnLong("ShardMinTime", shardDetails.map(x => x._2.minTime) ) )
    catalogue.add( DFHelper.createColumnLong("ShardMaxTime", shardDetails.map(x => x._2.maxTime) ) )
    catalogue.add( DFHelper.createColumnLong("CountPointsInShard", shardDetails.map(x => x._2.count)))

    val status = session.uploadTable(catalogue)

    println(s"${status}")

    session.close()
  }


  def main( args : Array[String]) : Unit ={

    val files = FileHelper.getListOfFiles("C:\\Earthwave\\data", "swath", ".csv")


    val gridCellSize = 100*1000
    //Return the file in row orientation
   files.foreach(x => createShards(DataFileParser.parseFile( x, gridCellSize ), gridCellSize))



  }

}
