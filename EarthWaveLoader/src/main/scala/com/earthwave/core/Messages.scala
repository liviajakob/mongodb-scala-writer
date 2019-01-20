package com.earthwave.core

object Messages {
  case class Completed(fileName : String)
  case class FileToProcess( file : java.io.File, gridCellSize : Int )
  case class Start()
  case class Stop()
  case class Flush()
  case class Finished()

}
