package com.earthwave.core

import java.time.LocalDateTime

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.earthwave.core.Messages.{Complete, CompletedSwathFile, FileToProcess, Flush}

class FileProcessor(idx : Int, shardManager: ActorRef ) extends Actor with ActorLogging{

  val parser = new DataFileParser()

  def receive ={
    case f : FileToProcess =>{
      log.info( s"Started processing file: ${f.file.getName}" )
      val now = LocalDateTime.now

      parser.parseFile(idx, f.file, f.gridCellSize, this)

      val end = LocalDateTime.now()

      log.info(s"File ${f.file.getName} processed in ${end.minusMinutes(now.getMinute).getMinute} mins ${end.minusSeconds(now.getSecond).getSecond}secs")

      sender ! CompletedSwathFile(f.file.getName())
    }
    case Flush()=>{
      parser.flush(shardManager)

      sender ! Complete()
    }

  }

}
