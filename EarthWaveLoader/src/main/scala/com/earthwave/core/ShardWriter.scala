package com.earthwave.core

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import akka.actor.{Actor, ActorLogging}
import com.earthwave.core.Messages.{CompletedGridCell, GridCellToProcess}
import com.stream_financial.core.data.DataType
import com.stream_financial.publib.connector.ConnectorSession
import com.stream_financial.publib.table.{DFColumn, DFTable}

class ShardWriter(x : Int ) extends Actor with ActorLogging {

  val longCols = Map("startTime" -> 0,"x" -> 1 ,"y" -> 2)
  val stringCols = Map("SwathFileName" -> 0 )

  override def receive ={

    case f : GridCellToProcess =>{

      log.info( s"Shard writer [$x] has received grid cell X=[${f.gridCell.x}] Y=[${f.gridCell.y}] to process" )

      def readFile( file : java.io.File ) : (FileHeader,Vector[String]) = {
        val bufferedSource = io.Source.fromFile(file)
        val header = bufferedSource.getLines().take(1)

        val headerFields: Iterator[Array[String]] =  for {line <- header
                                                          tokens = line.split(",")
        } yield tokens

        val flattenedHeader = headerFields.flatten.toVector

        val headerWithIndex = flattenedHeader.zip(Vector.range(0, flattenedHeader.length) )

        val fileHeader = FileHeader( file.getName, headerWithIndex  )

        //Drop the header
        val data = bufferedSource.getLines().drop(1).toVector

        bufferedSource.close()

        (fileHeader, data)
      }

      def batches( dataVector : Vector[String]  ) : List[Vector[String]] = {

        if( dataVector.length > Constants.shardSize + 100*1000 )
        {
          dataVector.take( Constants.shardSize) :: batches( dataVector.drop(Constants.shardSize) )
        }
        else
        {
          List(dataVector)
        }
      }

      def createColumns( cols : Vector[(String,Int)] ) : ( Vector[(Int,DFColumn[java.lang.Double])], Vector[(Int,DFColumn[java.lang.Long])], Vector[(Int,DFColumn[java.lang.String])] ) = {

        val longDFColumns = cols.filter( x => longCols.contains(x._1) ).map(col => ( col._2, new DFColumn[java.lang.Long](col._1, DataType.parse("Long")) ) )

        val stringDFColumns = cols.filter( x => stringCols.contains(x._1) ).map(col => ( col._2, new DFColumn[java.lang.String](col._1, DataType.parse("String")) ) )

        val doubleDFColumns = cols.filterNot( x => stringCols.contains(x._1)).filterNot(x => longCols.contains(x._1)).map(col => ( col._2, new DFColumn[java.lang.Double](col._1, DataType.parse("Double")) ) )

        (doubleDFColumns, longDFColumns, stringDFColumns)

      }

      def readAndUpload( batch : Vector[String], header : Vector[(String,Int)], outputDir : String ) : ShardDetail ={

        val dfColumns = createColumns(header)

        val rows = batch.map( line => line.split(",").toVector  )

        for( row <- rows )
        {
          dfColumns._1.foreach( x => x._2.add( if( row(x._1).isEmpty ){ null }else{ row(x._1).toDouble } ) )
          dfColumns._2.foreach( x => x._2.add( if( row(x._1).isEmpty ){ null }else{ row(x._1).toLong } ) )
          dfColumns._3.foreach( x => x._2.add( row(x._1) ) )
        }

        val session = new ConnectorSession(Constants.dfConnectorHost,  Constants.dfConnectorPort, outputDir )

        val dfTable = new DFTable( "Data" )

        dfColumns._1.foreach( x => dfTable.add(x._2) )
        dfColumns._2.foreach( x => dfTable.add(x._2) )
        dfColumns._3.foreach( x => dfTable.add(x._2) )

        val status = session.uploadTable(dfTable)

        log.info(s"$status")

        session.close()

        val xIndex = longCols("x")
        val yIndex = longCols("y")
        val tIndex = longCols("startTime")

        val minX = java.util.Collections.min(dfColumns._2(xIndex)._2)
        val maxX = java.util.Collections.max(dfColumns._2(xIndex)._2)
        val minY = java.util.Collections.min(dfColumns._2(yIndex)._2)
        val maxY = java.util.Collections.max(dfColumns._2(yIndex)._2)
        val minT = java.util.Collections.min(dfColumns._2(tIndex)._2)
        val maxT = java.util.Collections.max(dfColumns._2(tIndex)._2)
        val count : Long = dfColumns._2.head._2.size()

        val shardName = status.split(": ")(1)

        ShardDetail( "CryoSat",shardName, f.gridCell.x , f.gridCell.x + f.gridCell.size, f.gridCell.y , f.gridCell.y+f.gridCell.size, f.gridCell.size, minX , maxX , minY , maxY , minT , maxT, count  )
      }

      def writeCatalogue( shards : List[ShardDetail], outputDir : String) ={

        val session = new ConnectorSession(Constants.dfConnectorHost,  Constants.dfConnectorPort, outputDir )

        val catalogue = new DFTable( "Catalogue" )

        catalogue.add( DFHelper.createColumnString("dsName", shards.map(x => x.dsName ).toVector) )
        catalogue.add( DFHelper.createColumnString("CDBShardName", shards.map(x => x.shardName).toVector))
        catalogue.add( DFHelper.createColumnLong("GridCellMinX", shards.map(x => x.gridCellMinX ).toVector ) )
        catalogue.add( DFHelper.createColumnLong("GridCellMaxX", shards.map(x => x.gridCellMaxX ).toVector ) )
        catalogue.add( DFHelper.createColumnLong("GridCellMinY", shards.map(x => x.gridCellMinY ).toVector ) )
        catalogue.add( DFHelper.createColumnLong("GridCellMaxY", shards.map(x => x.gridCellMaxY ).toVector ) )
        catalogue.add( DFHelper.createColumnLong("GridCellSize", shards.map(s => s.gridCellSize).toVector ) )
        catalogue.add( DFHelper.createColumnLong("ShardMinX", shards.map(x => x.minX).toVector ) )
        catalogue.add( DFHelper.createColumnLong("ShardMaxX", shards.map(x => x.maxX).toVector ) )
        catalogue.add( DFHelper.createColumnLong("ShardMinY", shards.map(x => x.minY).toVector ) )
        catalogue.add( DFHelper.createColumnLong("ShardMaxY", shards.map(x => x.maxY).toVector ) )
        catalogue.add( DFHelper.createColumnLong("ShardMinTime", shards.map(x => x.minTime).toVector ) )
        catalogue.add( DFHelper.createColumnLong("ShardMaxTime", shards.map(x => x.maxTime).toVector ) )
        catalogue.add( DFHelper.createColumnLong("CountPointsInShard", shards.map(x => x.count).toVector))

        val status = session.uploadTable(catalogue)

        session.close()

        log.info(s"Catalogue written: ${status}")
        session.close()
      }

      val outputDate = LocalDate.parse( f.files.head.split("_")(1), DateTimeFormatter.ofPattern("yyyyMMdd") )

      val outputDir = s"EarthWave/${f.gridCell.x}_${f.gridCell.y}/${outputDate.getYear}/${outputDate.getMonthValue}"

      val allData = f.files.map( r => readFile( new java.io.File(r)) )

      val dataVector = allData.map(d => d._2 ).flatten.toVector

      val shards = batches( dataVector )

      log.info(s"For grid cell X=${f.gridCell.x} Y=${f.gridCell.y} will create ${shards.size} shards.")

      val shardDetails = shards.map( x => readAndUpload(x, allData.head._1.columns , outputDir) )

      writeCatalogue( shardDetails, outputDir )

      sender ! CompletedGridCell(f.gridCell)
    }
    case _ => {
      log.error( s"Unexpected message received." )
    }
  }


}
