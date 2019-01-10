package com.earthwave.core

import java.time.{LocalDate, LocalDateTime}
import java.util.concurrent.TimeUnit

import com.stream_financial.core.data.DataType
import com.stream_financial.core.datetime.DateTime
import com.stream_financial.publib.table.DFColumn

object DFHelper {

  def createColumnString(name: String, v: Vector[String]):DFColumn[String] = {

    val col = new DFColumn[String](name, DataType.parse("String"))
    v.foreach(col.add(_))
    col
  }

  def createColumnDouble( name : String, v : Vector[Double]) : DFColumn[Double] = {
    val col = new DFColumn[Double](name, DataType.parse("Double"))
    v.foreach(col.add(_))
    col
  }

  def createColumnNullableDouble( name : String, v : Vector[java.lang.Double] ) : DFColumn[java.lang.Double] ={
    val col = new DFColumn[java.lang.Double](name, DataType.parse("Double"))
    v.foreach(col.add(_))
    col
  }

  def createColumnInteger( name : String, v : Vector[Int]) : DFColumn[Int] = {
    val col = new DFColumn[Int](name, DataType.parse("Int"))
    v.foreach(col.add(_))
    col
  }

  def createColumnLong( name : String, v : Vector[Long]) : DFColumn[Long] = {
    val col = new DFColumn[Long](name, DataType.parse("Long"))
    v.foreach(col.add(_))
    col
  }

  def createColumnDateTime( name : String, v : Vector[LocalDateTime]) : DFColumn[DateTime] = {

    val dts = v.map( x => new DateTime( x.getYear, x.getMonthValue, x.getDayOfMonth, x.getHour, x.getMinute, x.getSecond, TimeUnit.MILLISECONDS.convert(x.getNano, TimeUnit.NANOSECONDS).toInt))
    val col = new DFColumn[DateTime](name, DataType.parse("DateTime"))
    dts.foreach(col.add( _ ))
    col
  }

  def createColumnDate( name : String, v : Vector[LocalDate]) : DFColumn[DateTime] = {

    val dts = v.map( x => new DateTime( x.getYear, x.getMonthValue, x.getDayOfMonth ) )
    val col = new DFColumn[DateTime](name, DataType.parse("DateTime"))
    dts.foreach(col.add( _ ))
    col
  }

}

