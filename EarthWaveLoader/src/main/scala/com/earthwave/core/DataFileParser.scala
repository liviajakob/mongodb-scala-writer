package com.earthwave.core

import akka.actor.{ActorLogging, ActorRef}
import org.mongodb.scala._

import scala.concurrent.Await
import scala.concurrent.duration._

case class FileHeader( fileName : String, columns : Vector[(String,Int)] )
{
  def getIndex( name : String ): Int =
  {
    columns.filter(x => x._1 == name).head._2
  }

}

class DataFileParser {


  def parseFile( idx : Int, file : java.io.File, gridCellSize : Long, log : ActorLogging )  = {
    // create the data tables
    val bufferedSource = io.Source.fromFile(file)

    val lines = bufferedSource.getLines()
    val header = lines.take(1)

    val headerFields: Iterator[Array[String]] =  for {line <- header
                                                      tokens = line.split(",")
    } yield tokens

    //Dropping the first column.e
    val headers =  headerFields.flatten.toVector.drop(1).toList
    val headerWithIndex = headers.zip(Vector.range(0, headers.length) ).toVector

    val mongoClient: MongoClient = MongoClient()
    val database: MongoDatabase = mongoClient.getDatabase(Constants.mongoDBname)


    def parseLine( l :String, xIndex : Int, yIndex : Int, fileHeader : FileHeader ) : Unit =
    {
      val tokens = l.split(",").drop(1).toList

      val values: Vector[java.lang.Double] = tokens.map(x => FileHelper.parseField(x)).toVector

      // generate collection name
      val cellX = Math.floor(values(xIndex) / gridCellSize).toLong * gridCellSize
      val cellY = Math.floor(values(yIndex) / gridCellSize).toLong * gridCellSize
      val gridCell = GridCell(cellX,cellY,gridCellSize.toLong)
      val fileLoadDate = fileHeader.fileName.replace("swath_","").split("-" )(0)
      val year = fileLoadDate.substring(0,4)
      val month = fileLoadDate.substring(4,6)
      val collectionName = s"${Constants.dataName}_y${year}_m${month}_x${gridCell.x}_y${gridCell.y}"

      // create / call the collection
      val collection: MongoCollection[Document] = database.getCollection(collectionName)

      // create a map with headers as key and tokens as values
      val docMap = (headers :+ "fileName") zip (tokens :+ fileHeader.fileName) toMap

      // create
      val obs = collection.insertOne(Document(docMap))
      obs.subscribe(new Observer[Completed] {
        override def onNext(result: Completed): Unit = {} //println(s"Next: $result")
        override def onError(e: Throwable): Unit = println(s"Error: $e")
        override def onComplete(): Unit = {} //println("Completed")
      })

      // keep it synchronous within one thread
      Await.result(obs.toFuture, Duration.Inf)


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

    /*buckettedRows.values.foreach(x => { x.flush()
                                        shardManager ! GridCellFile( x.gridCell, x.dfName, x.rowCount )
                                      })*/
  }

}
