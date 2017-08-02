package unionpay.bussiness.poc.quasirealtimeclearing

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by zhaikaixuan on 28/07/2017.
  */

trait QueryRelatedProperty{
  def queryProperty(targetCol:String, sourceColName:String, sourceColValue:String):String
}

class QueryRelatedPropertyInDF(sqlContext: SQLContext,tableName:String) extends QueryRelatedProperty{
  private lazy val df = sqlContext.load(tableName)
  override def queryProperty(targetCol: String, sourceColName: String, sourceColValue:String): String = {
    val value = df.where(s"$sourceColName = $sourceColValue").select(targetCol).first()
    value.toString
  }
}
