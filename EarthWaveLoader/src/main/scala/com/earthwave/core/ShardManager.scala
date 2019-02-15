package com.earthwave.core

import akka.actor.{Actor, ActorLogging, Props}
import com.earthwave.core.Messages._


class ShardManager extends Actor with ActorLogging {

  var shards = Map[GridCell,List[String]]()

  var gridCellsToProcess = List[GridCellToProcess]()
  var processingGridCells = Set[GridCell]()

  //Required Grid Cell Processing Actors
  val numberOfActors = Constants.numberOfShardWriters

  //Create the ids for the processing actors
  val range = List.range[Int](0,numberOfActors,1)

  //Create the processing actors
  val processingActors = range.map(x => context.actorOf(Props(new ShardWriter( x, self )), s"ShardWriter$x"))

  var completedFileProcessors = 0

  var gridCellFilesReceived = 0

  var totalRowCount : Long = 0

  private def nextGridCell() : GridCellToProcess ={
    val nextGridCell = gridCellsToProcess.head
    gridCellsToProcess = gridCellsToProcess.drop(1)
    processingGridCells = processingGridCells.+(nextGridCell.gridCell)

    nextGridCell
  }

  override def receive = {

    case Start() => {
      gridCellsToProcess = shards.toList.map( x => GridCellToProcess( x._1, x._2 ))
      log.info(s"Shard Manager Start received [${gridCellsToProcess.size}] grid cells to process. Received ${gridCellFilesReceived} grid cell files.")

      processingActors.foreach( x =>
                                     if( !gridCellsToProcess.isEmpty)
                                     {
                                       val gridCell = nextGridCell()
                                       x ! gridCell
                                     })

      log.info( s"Total number of data points: ${totalRowCount}")
    }
    case f : GridCellFile => {
      if( shards.contains(f.gridCell))
      {
          val files = shards(f.gridCell)
          val updatedFiles = f.fileName :: files

          shards = shards.+( (f.gridCell, updatedFiles) )
      }
      else
      {
        shards = shards.+((f.gridCell,List(f.fileName)) )
      }

      gridCellFilesReceived = gridCellFilesReceived + 1
      totalRowCount = f.rowCount + totalRowCount
    }
    case c : CompletedGridCell => {
      processingGridCells = processingGridCells.-(c.gridCell)

      if( !gridCellsToProcess.isEmpty )
      {
        val gridCell = nextGridCell()
        sender ! gridCell
        log.info(s"Number of grid cells remaining to process: ${gridCellsToProcess.size}")
      }
      else
      {
        log.info( s"No more grid cells to process. Still processing ${processingGridCells.size}" )
      }
    }
    case Finished() =>{
      if( gridCellsToProcess.isEmpty && processingGridCells.isEmpty )
      {
          log.info(s"Stopping the shard writers")
          processingActors.foreach(x => context.stop(x) )

          sender ! true
      }
      else
      {
        log.info(s"GridCells ${gridCellsToProcess.size} still to write.")
        sender ! false
      }
    }
  }

}
