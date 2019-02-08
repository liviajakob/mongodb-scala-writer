package com.earthwave.core

object Messages {
  case class CompletedSwathFile(fileName : String)
  case class CompletedGridCell( gridCell : GridCell )
  case class Complete()
  case class FileToProcess( file : java.io.File, gridCellSize : Long)
  case class GridCellFile( gridCell : GridCell, fileName : String, rowCount : Long )
  case class GridCellToProcess( gridCell : GridCell, files : List[String] )
  case class Start()
  case class Stop()
  case class Flush()
  case class Finished()
}
