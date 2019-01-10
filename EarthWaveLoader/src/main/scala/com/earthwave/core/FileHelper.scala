package com.earthwave.core

import java.io.File

object FileHelper {

  def getListOfFiles(dir: String, startsWith : String, endsWith : String) : List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.filter(_.getName.endsWith(endsWith)).filter(_.getName.startsWith(startsWith))
    }
    else{ List[File]() }
  }


}
