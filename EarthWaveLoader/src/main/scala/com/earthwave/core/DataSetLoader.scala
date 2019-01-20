package com.earthwave.core

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import com.earthwave.core.Messages.{Finished, Start}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Await


case class DataSetLoaderConfig( inputFilePath : String, startsWith : String, ext : String, parallelisation : Int, gridCellSize : Int  )

object DataSetLoader {
  def main(args: Array[String]): Unit = {

    val conf: Config = ConfigFactory.load()
    implicit val system = ActorSystem("Process", conf)
    implicit val timeout = Timeout( 5 seconds )

    val gridCellSize = 100 * 1000

    //Start the file manager
    val dataSetLoaderConfig = DataSetLoaderConfig("C:\\Earthwave\\data", "swath",".csv", 4, gridCellSize)

    val fileManagerActor = system.actorOf(Props(new FileManager(dataSetLoaderConfig)),"FileManager")

    fileManagerActor ! Start()

    while( !Await.result(( fileManagerActor ? Finished() ).mapTo[Boolean], 30 seconds) )
    {
      Thread.sleep(60 * 1000 )
    }
    system.terminate()
  }
}

