package com.ums

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by zhaikaixuan on 28/07/2017.
  */

trait QueryRelatedProperty{
  def queryProperty(tableName:String, targetCol:String, sourceColName:String, sourceColValue:String):String
}

class QueryRelatedPropertyInDF(df:DataFrame) extends QueryRelatedProperty{
  override def queryProperty(tableName:String, targetCol: String, sourceColName: String, sourceColValue:String): String = {
    val value = df.where(s"$sourceColName = $sourceColValue").select(targetCol).first()
    value.toString
  }
}
