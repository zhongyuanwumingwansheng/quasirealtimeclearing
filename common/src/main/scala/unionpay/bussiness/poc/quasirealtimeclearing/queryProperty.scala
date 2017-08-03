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
  def queryPropertyInBMS_STL_INFODF(targetCol: String, sourceColName: String, sourceColValue:String): Option[String] = {
    val df2 = df.where(s"$sourceColName = $sourceColValue")
    if (df2.count() != 0){
      df2.registerTempTable("BMS_STL_INFO")
      val sqlStatement = s"select $targetCol from BMS_STL_INFO order by decode(trim(mapp_main), '1', 1, 2), " +
        s"decode(apptype_id, 1,1,86,2,74,3,18,4,39,5,40,6,68,7)"
      Some(sqlContext.sql(sqlStatement).first().toString())
    }
    else
      None
  }
}
