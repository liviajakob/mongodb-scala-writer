package com.earthwave.core

import java.io.File

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.earthwave.core.Messages._

import scala.concurrent.Await
import scala.concurrent.duration._

class FileManager( config : DataSetLoaderConfig ) extends Actor with ActorLogging {

  //Get the list of files to process
  var files = FileHelper.getListOfFiles(config.inputFilePath, config.startsWith,config.ext)
  var processingFiles = Set[String]()

  log.info(s"Found [${files.size}] to process.")

  //Required File Processing Actors
  val numberOfActors = Math.min( config.parallelisation, files.size )

  //Create the ids for the processing actors
  val range = List.range[Int](0,numberOfActors,1)
  //Create the processing actors
  val catalogueBuilder = context.actorOf( Props( new CatalogueBuilder()), "CatalogueBuilder" )

  val processingActors = range.map(x => context.actorOf(Props(new FileProcessor(x, catalogueBuilder)), s"FileProcessor$x"))

  private def nextFile() : File ={
    val nextFile = files.head
    files = files.drop(1)
    processingFiles = processingFiles.+(nextFile.getName)

    nextFile
  }

  override def receive ={
    case Start() =>{
      log.info("File Manager start received")
      processingActors.foreach( a => a ! FileToProcess(nextFile(),config.gridCellSize ))
    }
    case c : Completed =>
    {
      log.info( s"Completed processing file: ${c.fileName}" )
      processingFiles = processingFiles.-(c.fileName)

      if(!files.isEmpty) {
        sender ! FileToProcess(nextFile(), config.gridCellSize)
      }
      else {
        log.info(s"No more files to process.")
      }
    }
    case Finished() =>{
      log.info(s"Heartbeat received. Files to process=${files.size} Files Processing=${processingFiles.size}")

      if( files.isEmpty && processingFiles.isEmpty)
      {
        implicit val timeout = Timeout( 5 seconds )
        //Flush the catalogue builder
        Await.result((catalogueBuilder ? Flush()), 5 seconds)

        //Stop the processing actors
        log.info("Completed processing now stopping the file processors.")
        processingActors.foreach( x => context.stop(x) )

        context.stop(catalogueBuilder)
        sender ! true
      }
      else
      {
        sender ! false
      }
    }


  }
}
