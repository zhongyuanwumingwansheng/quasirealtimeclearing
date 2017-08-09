package ums.bussiness.realtime.common

import org.apache.spark.SparkContext
import com.typesafe.config._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import ums.bussiness.realtime.model.table.BmsStInfo
import ums.bussiness.realtime.model.table._
/**
  * Created by zhaikaixuan on 08/08/2017.
  */
object ParseTable {
  def parseTable(sc: SparkContext, sqlContext: SQLContext, setting: Config):(DataFrame, DataFrame, DataFrame, DataFrame) = {
    val BmsStInfoRdd = sc.textFile(setting.getString("BmsStInfo.BmsStInfoLoc")).map(_.replace("|||", "").trim.split("|")).map {
      row =>
        new BmsStInfo(row(setting.getInt("BmsStInfo.merIdIndex")),
          row(setting.getInt("BmsStInfo.merNoIndex")),
          row(setting.getInt("BmsStInfo.mapMainIndex")),
          row(setting.getInt("BmsStInfo.apptypeidIndex")).toInt,
          row(setting.getInt("BmsStInfo.creditCalTypeIndex")),
          row(setting.getInt("BmsStInfo.creditCalcRateIndex")).toDouble,
          row(setting.getInt("BmsStInfo.creditCalAmtIndex")).toDouble,
          row(setting.getInt("BmsStInfo.creditMinAmtIndex")).toDouble,
          row(setting.getInt("BmsStInfo.creditMaxAmtIndex")).toDouble)
    }
    val BmsStInfoRDF = sqlContext.createDataFrame(BmsStInfoRdd, classOf[BmsStInfo])

    val SysGroupItemInfoRdd = sc.textFile(setting.getString("SysGroupItemInfo.SysGroupItemInfoLoc")).
      map(_.split("|")).map {
      row =>
        new SysGroupItemInfo(row(setting.getInt("SysGroupItemInfo.instIdIndex")),
          row(setting.getInt("SysGroupItemInfo.batDateIndex")),
          row(setting.getInt("SysGroupItemInfo.groupIdIndex")),
          row(setting.getInt("SysGroupItemInfo.groupTypeIdIndex")),
          row(setting.getInt("SysGroupItemInfo.itemIndex")),
          row(setting.getInt("SysGroupItemInfo.becomeEffectiveDateIndex")),
          row(setting.getInt("SysGroupItemInfo.lostEffectiveDateIndex")),
          row(setting.getInt("SysGroupItemInfo.rcdVerIndex")).toInt,
          row(setting.getInt("SysGroupItemInfo.addDatetimeIndex")),
          row(setting.getInt("SysGroupItemInfo.addUserIdIndex")),
          row(setting.getInt("SysGroupItemInfo.updDatetimeIndex")),
          row(setting.getInt("SysGroupItemInfo.updUserIdIndex")))
    }
    val SysGroupItemInfoDF = sqlContext.createDataFrame(SysGroupItemInfoRdd, classOf[SysGroupItemInfo])

    val SysTxnCdInfoRdd = sc.textFile(setting.getString("SysTxnCdInfo.SysTxnCdInfoLoc")).map(_.split("|")).map {
      row =>
        new SysTxnCdInfo(row(setting.getInt("SysTxnCdInfo.txnKeyIndex")),
          row(setting.getInt("SysTxnCdInfo.txnCodeIndex")),
          row(setting.getInt("SysTxnCdInfo.txnDesIndex")),
          row(setting.getInt("SysTxnCdInfo.bmsTxnCodeIndex")),
          row(setting.getInt("SysTxnCdInfo.settFlgIndex")),
          row(setting.getInt("SysTxnCdInfo.dcFlgIndex")).toInt,
          row(setting.getInt("SysTxnCdInfo.txnCodeGrp")),
          row(setting.getInt("SysTxnCdInfo.rcdVerIndex")).toInt,
          row(setting.getInt("SysTxnCdInfo.addDatetimeIndex")),
          row(setting.getInt("SysTxnCdInfo.addUserIdIndex")),
          row(setting.getInt("SysTxnCdInfo.updDatetimeIndex")),
          row(setting.getInt("SysTxnCdInfo.updUserIdIndex")))
    }
    val SysTxnCdInfoDF = sqlContext.createDataFrame(SysTxnCdInfoRdd, classOf[SysTxnCdInfo])

    val SysMapItemInfoRdd = sc.textFile(setting.getString("SysMapItemInfo.SysMapItemInfoLoc")).map(_.split("|")).map {
      row =>
        new SysMapItemInfo(row(setting.getInt("SysMapItemInfo.mapIdIndex")),
          row(setting.getInt("SysMapItemInfo.srcItemIndex")),
          row(setting.getInt("SysMapItemInfo.mapResultIndex")))
    }
    val SysMapItemInfoDF = sqlContext.createDataFrame(SysMapItemInfoRdd, classOf[SysMapItemInfo])
    (BmsStInfoRDF, SysGroupItemInfoDF, SysTxnCdInfoDF, SysMapItemInfoDF)
  }
}
