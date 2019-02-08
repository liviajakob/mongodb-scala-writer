package com.earthwave.core

import java.io.{BufferedWriter, File, FileWriter}



case class DataFile(sourceFileHeader : FileHeader, gridCell : GridCell, data : java.io.File )


class DataFileWriter( idx : Int, sourceFileHeader : FileHeader, val gridCell : GridCell ){

  val fileLoadDate = sourceFileHeader.fileName.replace("swath_","").split("-" )(0)
  val dfName = s"${Constants.intermediatePath}${idx}_${fileLoadDate}_${gridCell.x}_${gridCell.y}.csv"

  val df = new File( dfName )
  val bw : BufferedWriter = new BufferedWriter(new FileWriter(df, false ))

  var rowCount : Long = 0
  //write the header
  sourceFileHeader.columns.map( x => x._1 ).foreach( x => bw.write( x + ",") )
  bw.write("SwathFileName")
  bw.newLine()

  def write( line : String  ) = {
                                        rowCount = rowCount + 1
                                        bw.write(line)
                                        bw.newLine()
                                      }
  def flush() = {
    bw.flush()
    bw.close()
  }
}