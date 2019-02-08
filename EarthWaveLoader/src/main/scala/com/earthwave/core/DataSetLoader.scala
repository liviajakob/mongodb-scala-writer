package com.earthwave.core

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import com.earthwave.core.Messages.{Finished, Start}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Await


case class DataSetLoaderConfig( inputFilePath : String, startsWith : String, ext : String, parallelisation : Int, gridCellSize : Int  )

object Constants
{
  val dfConnectorHost = "localhost"
  val dfConnectorPort = 9001
  val dataOutputPath = "Earthwave//Data0"
  val catalogueOutputPath = "Earthwave//Data0"

  val intermediatePath = "c:\\EarthWave\\Intermediate\\"
  val shardSize = 500 * 1000
  val numberOfShardWriters = 4
}


object DataSetLoader {
  def main(args: Array[String]): Unit = {

    val conf: Config = ConfigFactory.load()
    implicit val system = ActorSystem("Process", conf)
    implicit val timeout = Timeout( 5 seconds )

    val gridCellSize = 100 * 1000

    //Start the file manager
    val dataSetLoaderConfig = DataSetLoaderConfig("C:\\Earthwave\\Data", "swath",".csv", 6, gridCellSize)

    val shardManager = system.actorOf(Props(new ShardManager()), "ShardManager" )

    val fileManagerActor = system.actorOf(Props(new FileManager(dataSetLoaderConfig, shardManager)),"FileManager")

    fileManagerActor ! Start()

    while( !Await.result(( fileManagerActor ? Finished() ).mapTo[Boolean], 30 seconds) )
    {
      Thread.sleep(15 * 1000 )
    }

    while( !Await.result( (shardManager ? Finished() ).mapTo[Boolean], 30 seconds ) )
    {
      Thread.sleep(15 * 1000 )
    }

    system.terminate()
  }
}

