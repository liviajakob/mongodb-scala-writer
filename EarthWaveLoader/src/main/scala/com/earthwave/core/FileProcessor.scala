package com.earthwave.core

import java.time.LocalDateTime

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.earthwave.core.Messages.{Completed, FileToProcess}

class FileProcessor(id : Int, catalogueBuilder : ActorRef) extends Actor with ActorLogging{

  def receive ={
    case f : FileToProcess =>{
      log.info( s"[$id] Started processing file: ${f.file.getName}" )
      val now = LocalDateTime.now

      ShardBuilder.createShards(DataFileParser.parseFile(f.file, f.gridCellSize, this), f.gridCellSize, this, catalogueBuilder : ActorRef)

      val end = LocalDateTime.now()

      log.info(s"File ${f.file.getName} processed in ${end.minusMinutes(now.getMinute).getMinute} mins ${end.getSecond}secs")

      sender ! Completed(f.file.getName())
    }

  }

}
