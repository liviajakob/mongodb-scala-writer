package com.earthwave.core

import akka.actor.{ActorLogging, ActorRef}
import com.earthwave.core.Messages.GridCellFile

import scala.collection.mutable.Map

case class FileHeader( fileName : String, columns : Vector[(String,Int)] )
{
  def getIndex( name : String ): Int =
  {
    columns.filter(x => x._1 == name).head._2
  }

}

class DataFileParser {

  var buckettedRows = Map[GridCell, DataFileWriter]()

  def parseFile( idx : Int, file : java.io.File, gridCellSize : Long, log : ActorLogging )  = {
    // create the datatables
    val bufferedSource = io.Source.fromFile(file)

    val lines = bufferedSource.getLines()
    val header = lines.take(1)

    val headerFields: Iterator[Array[String]] =  for {line <- header
                                                      tokens = line.split(",")
    } yield tokens

    //Dropping the first column.
    val headers =  headerFields.flatten.toVector.drop(1).toList

    val headerWithIndex = headers.zip(Vector.range(0, headers.length) ).toVector

    def parseLine( l :String, xIndex : Int, yIndex : Int, fileHeader : FileHeader ) : Unit =
    {
      val tokens = l.split(",").drop(1).toList

      val values: Vector[java.lang.Double] = tokens.map(x => FileHelper.parseField(x)).toVector

      val cellX = Math.floor(values(xIndex) / gridCellSize).toLong * gridCellSize
      val cellY = Math.floor(values(yIndex) / gridCellSize).toLong * gridCellSize
      val gridCell = GridCell(cellX,cellY,gridCellSize.toLong)

      if( buckettedRows.contains(gridCell))
      {
        val writer = buckettedRows(gridCell)
        val line = tokens.reduceLeft( (x,y)=> x.concat(s",$y"))
        val lineWithSwath = line.concat(s",${file.getName}")
        writer.write(lineWithSwath)
      }
      else
      {
        val dfw = new DataFileWriter(idx, fileHeader, gridCell )
        val line = tokens.reduceLeft( (x,y)=> x.concat(s",$y"))
        val lineWithSwath = line.concat(s",${file.getName}")
        dfw.write(lineWithSwath)

        buckettedRows.+=((gridCell, dfw))
      }
    }

    val fileHeader = FileHeader(file.getName(), headerWithIndex)

    val xIndex = fileHeader.getIndex("x")
    val yIndex = fileHeader.getIndex("y")

    val t = Profile.profile(
    lines.foreach( l => parseLine(l, xIndex, yIndex, fileHeader ))
    )

    log.log.info( s"File ${file.getName()} took ${t._2} millis to parse" )

  }

  def flush(shardManager : ActorRef) = {

    buckettedRows.values.foreach(x => { x.flush()
                                        shardManager ! GridCellFile( x.gridCell, x.dfName, x.rowCount )
                                      })
  }

}
