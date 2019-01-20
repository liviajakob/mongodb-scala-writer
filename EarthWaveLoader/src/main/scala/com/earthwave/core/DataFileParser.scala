package com.earthwave.core

import akka.actor.ActorLogging


case class FileHeader( fileName : String, columns : Vector[(String,Int)] )
{

  def getIndex( name : String ): Int =
  {
    columns.filter(x => x._1 == name).head._2
  }

}




object DataFileParser {


  def parseFile( file : java.io.File, gridCellSize : Long, log : ActorLogging ) : (FileHeader,Map[GridCell,Vector[Vector[java.lang.Double]]]) = {
    var buckettedRows : Map[GridCell, Vector[Vector[java.lang.Double]]] = Map[GridCell, Vector[Vector[java.lang.Double]]]()
    // create the datatables
    val bufferedSource = io.Source.fromFile(file)

    val header = bufferedSource.getLines().take(1)

    val headerFields: Iterator[Array[String]] =  for {line <- header
                                                      tokens = line.split(",")
    } yield tokens

    //Dropping the first column.
    val headers =  headerFields.flatten.toVector.drop(1)

    val headerWithIndex = headers.zip(Vector.range(0, headers.length) )

    //get the lines without the header.
    val dataLines = bufferedSource.getLines().drop(1)

    def parseLine( l :String, xIndex : Int, yIndex : Int ) : Unit =
    {
      def parseField( f : String) : java.lang.Double = f match {

        case x : String if x.isEmpty => null;
        case y : String => y.toDouble;
      }

      val tokens = l.split(",").drop(1)

      val values: Vector[java.lang.Double] = tokens.map(x => parseField(x)).toVector

      val cellX = Math.floor(values(xIndex) / gridCellSize).toLong * gridCellSize
      val cellY = Math.floor(values(yIndex) / gridCellSize).toLong * gridCellSize
      val gridCell = GridCell(cellX,cellY,gridCellSize.toLong)

      if( buckettedRows.contains(gridCell))
      {
        val rowData : Vector[Vector[java.lang.Double]] = buckettedRows(gridCell)
        val element = ( gridCell, rowData.:+(values))

        buckettedRows = buckettedRows.+( element )
      }
      else
      {
        val element = (GridCell(cellX, cellY, gridCellSize ), Vector(values))
        buckettedRows = buckettedRows.+(element)
      }
    }

    val fileHeader = FileHeader(file.getName(), headerWithIndex)

    val xIndex = fileHeader.getIndex("x")
    val yIndex = fileHeader.getIndex("y")

    val t = Profile.profile(
    dataLines.foreach( l => parseLine(l, xIndex, yIndex ))
    )

    log.log.info( s"File ${file.getName()} took ${t._2} millis to parse" )

    (FileHeader(file.getName(), headerWithIndex ), buckettedRows)
  }

}
