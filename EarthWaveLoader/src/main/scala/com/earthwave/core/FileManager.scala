package com.earthwave.core

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import com.earthwave.core.Messages._

class FileManager( config : DataSetLoaderConfig, shardManager : ActorRef ) extends Actor with ActorLogging {

  //Get the list of files to process
  var files = FileHelper.getListOfFiles(config.inputFilePath, config.startsWith,config.ext)
  var processingFiles = Set[String]()

  log.info(s"Processing Folder: ${config.inputFilePath}")
  log.info(s"Found [${files.size}] to process.")

  //Required File Processing Actors
  val numberOfActors = Math.min( config.parallelisation, files.size )

  //Create the ids for the processing actors
  val range = List.range[Int](0,numberOfActors,1)
  //Create the processing actors

  val processingActors = range.map(x => context.actorOf(Props(new FileProcessor( x, shardManager )), s"FileProcessor$x"))

  var completedFileProcessors = 0

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
    case c : CompletedSwathFile =>
    {
      log.info( s"Completed processing file: ${c.fileName}" )
      processingFiles = processingFiles.-(c.fileName)

      if(!files.isEmpty) {
        sender ! FileToProcess(nextFile(), config.gridCellSize)

      }
      else {
        log.info(s"No more files to process.")
        sender ! Flush()
      }
    }
    case Complete() =>
    {
      completedFileProcessors = completedFileProcessors + 1
    }
    case Finished() =>{
      log.info(s"Heartbeat received. Files to process=${files.size} Files Processing=${processingFiles.size}")

      if( files.isEmpty && processingFiles.isEmpty && completedFileProcessors == numberOfActors )
      {
        //Stop the processing actors
        log.info("Completed processing now stopping the file processors.")

        processingActors.foreach( x => context.stop(x) )

        log.info("Now start the Shard Managers Processing" )
        shardManager ! Start()

        sender ! true
      }
      else
      {
        sender ! false
      }
    }
  }
}
