package ums.bussiness.realtime

import java.util
import java.util.Random

import com.typesafe.config._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.query.{Query, SqlQuery}
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.ShortTypeHints
import org.json4s.jackson.Serialization
import org.json4s.native.JsonMethods._
import ums.bussiness.realtime.model.flow.UlinkNormal
import java.util.Date
import scala.collection.mutable.{ArrayBuffer, HashMap}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{Ignite, IgniteCache}
import org.apache.spark.rdd.RDD
import ums.bussiness.realtime.common.{HbaseUtil, IgniteFunction, IgniteUtil}
import ums.bussiness.realtime.model.table.{BmsStInfo, SysGroupItemInfo, SysMapItemInfo, SysTxnCdInfo}

import scala.collection.mutable

/**
  * Created by zhaikaixuan on 27/07/2017.
  */
object ApplyULinkNormalRuleToSparkStream2 extends Logging {
  implicit val format = Serialization.formats(ShortTypeHints(List()))
  private val CONFIG = "example-ignite.xml"
  private val cacheName = "records"
  private val SYS_TXN_CODE_INFO_CACHE_NAME = "SysTxnCdInfo"
  private val SYS_GROUP_ITEM_INFO_CACHE_NAME = "SysGroupItemInfo"
  private val SYS_MAP_ITEM_INFO_CACHE_NAME = "SysMapItemInfo"
  private val BMS_STL_INFO_CACHE_NAME = "BmsStInfo"
  private val HISTORY_RECORD_TABLE = "ulink_normal_table"
  private val SUMMARY = "NormalSummary"

  case class MerchantHisAmout(@QuerySqlField(index = true) merchant_no: String, @QuerySqlField(index = false) amout: Double)

  def main(args: Array[String]): Unit = {
    val setting: Config = ConfigFactory.load()
    //read data from kafka
    val kafkaZkHost = setting.getString("kafkaZkHost")
    val appName = setting.getString("appName")
    val processInterval = setting.getInt("processInterval")
    val logLevel = setting.getString("logLevel")
    val kafkaTopics = setting.getString("kafkaTopics")
    val kafkaReceiverNum = setting.getInt("kafkaReceiverNum")
    val kafkaGroup = setting.getString("kafkaGroup")
    val kafkaThread = setting.getInt("kafkaThread")
    val conf = new SparkConf().setMaster("local[3]").setAppName(appName)
    val streamContext = new StreamingContext(conf, Milliseconds(processInterval))
    streamContext.sparkContext.setLogLevel(logLevel)
    val sc = streamContext.sparkContext
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    val ulinkNormalTopicMap = scala.collection.immutable.Map("ULinkNormal" -> 1)
    //val topicMap = kafkaTopics.split(",").map((_, kafkaThread)).toMap
    val kafkaStreams = (1 to kafkaReceiverNum).map { _ =>
      KafkaUtils.createStream(streamContext, kafkaZkHost, kafkaGroup, ulinkNormalTopicMap, StorageLevels.MEMORY_AND_DISK_SER)
    }
    val lines = streamContext.union(kafkaStreams).map(_._2)

    //lines.print

    //cache for summary
    val cacheName = "summary"
    val ignite = IgniteUtil(setting)
    destroyCache$(SUMMARY)
    createCache$(SUMMARY, indexedTypes = Seq(classOf[String], classOf[Double]))

    //解析数据
    val records = lines.map(line => {
      val record = new HashMap[String, String]
      val formatLine = line.replaceAll("\":null\"", ":\"\"")
      val ulinkNormal = new UlinkNormal()
      try {
        val JObject(message) = parse(formatLine).asInstanceOf[JObject]
        message.map(keyValue =>
          keyValue._1 match {
            case "ID" => ulinkNormal.setId(keyValue._2.extract[String])
            case "MSG_TYPE" => ulinkNormal.setMsgType(keyValue._2.extract[String])
            case "PROC_CODE" => ulinkNormal.setProcCode(keyValue._2.extract[String])
            case "SER_CONCODE" => ulinkNormal.setSerConcode(keyValue._2.extract[String])
            case "TXN_AMT" => ulinkNormal.setTxnAmt(keyValue._2.extract[String].toDouble)
            case "TID" => ulinkNormal.settId(keyValue._2.extract[String])
            case "MID" => ulinkNormal.setmId(keyValue._2.extract[String])
            case "TRAN_STATUS" => ulinkNormal.setTranStat(keyValue._2.extract[String])
            case "RESP_CODE" => ulinkNormal.setRespCode(keyValue._2.extract[String])
            case "RSV4" => ulinkNormal.setRSV4(keyValue._2.extract[String])
            case _ =>
          }
        )
      } catch {
        case e: Exception => logError("Parse Json ERROR: " + formatLine)
      }
      ulinkNormal
    })

    //交易过滤
    val filterRecoders = records.mapPartitions {

      val filterRecords = new ArrayBuffer[UlinkNormal]
      iter =>
        val cacheName = "records" + new Random(10).longs()
        val ignite = IgniteUtil(setting)
        destroyCache$(cacheName)
        createCache$(cacheName, indexedTypes = Seq(classOf[String], classOf[UlinkNormal]))
        iter.foreach { record =>
          cache$(cacheName).get.put(record.getId(), record)
        }
//        val sql = "(UlinkNormal.procCode != 01 and UlinkNormal.procCode != 02)"
        val sql = "(UlinkNormal.procCode != \'01\' and UlinkNormal.procCode != \'02\')" +
          " or (UlinkNormal.respCode != \'00\' and UlinkNormal.respCode != \'10\' and UlinkNormal.respCode != \'11\' and UlinkNormal.respCode != \'16\' and UlinkNormal.respCode != \'A2\' " +
          "and UlinkNormal.respCode != \'A4\' and UlinkNormal.respCode != \'A5\' and UlinkNormal.respCode != \'A6\' and UlinkNormal.respCode != \'Y1\' and UlinkNormal.respCode != \'Y3\') " +
          " or (UlinkNormal.tranStat != \'1\' and UlinkNormal.tranStat != \'2\' and UlinkNormal.tranStat != \'3\')"
        val result = cache$[String, UlinkNormal](cacheName).get.sql(sql).getAll
        val result_iterator = result.iterator()
        while (result_iterator.hasNext) {
          val record = result_iterator.next()
          filterRecords += record.getValue
        }
        cache$(cacheName).get.destroy()
        filterRecords.iterator
    }
    //交易码转换
    val transRecords = filterRecoders.map { record =>
      val ignite = IgniteUtil(setting)
      addCacheConfig(ignite)
      val filed = record.getMsgType + "|" + record.getProcCode + "|" + record.getSerConcode
      val query_sql = "SysTxnCdInfo.txnKey = \'" + filed + "\'"
      val queryResult = cache$[String, SysTxnCdInfo](SYS_TXN_CODE_INFO_CACHE_NAME).get.sql(query_sql).getAll
      println("SysTxnCdInfo  has " + queryResult.size() + " result by the txnKey = " + filed)
      if (queryResult.size > 0) {
        val filterFlag = queryResult.get(0).getValue.getSettFlg match {
          case "0" | "7" | "8" | "E" | "F" | "H" | "N" | "Z" => false
          case "1" | "2" | "3" | "4" | "5" | "6" => true
          case _ => false
        }
        val dcFlag = queryResult.get(0).getValue.getDcFlg
        record.setFilterFlag(filterFlag)
        record.setDcFlag(dcFlag)
      }
      record
    }

    //清分规则定位与计算
    val mapRecords = transRecords.map { record =>
      //清分规则 ID 获取,可能不止一个，所以通过逗号进行拼接
      val query_group_sql = s"SysGroupItemInfo.item = \'${record.getmId}\'";
      val queryResult = cache$[String, SysGroupItemInfo](SYS_GROUP_ITEM_INFO_CACHE_NAME).get.sql(query_group_sql).getAll
      println("SysGroupItemInfo  has " + queryResult.size() + " result by the query_group_sql ： " + query_group_sql)
      var append_groupId: List[String] = List()
      val resultIterator = queryResult.iterator()
      while (resultIterator.hasNext) {
        val groupId = resultIterator.next().getValue.getGroupId
        append_groupId = groupId :: append_groupId
      }
      record.setGroupId(append_groupId.mkString(","))

      //按终端入账
      var query_mer_filed = record.getmId + "," + record.gettId
      var query_mer_sql = s"SysMapItemInfo.srcItem = \'${query_mer_filed}\' and SysMapItemInfo.typeId = \'1\'";
      var queryMerResult = cache$[String, SysMapItemInfo](SYS_MAP_ITEM_INFO_CACHE_NAME).get.sql(query_mer_sql).getAll
      println("SysMapItemInfo  has " + queryResult.size() + " result by the query_mer_sql = " + query_mer_sql)
      if (queryMerResult.size > 0) {
        val mapId = queryMerResult.get(0).getValue.getMapId
        val mapResult = queryMerResult.get(0).getValue.getMapResult
        if (mapResult != ""){
          record.setMerNo(mapResult)
        }
      }

      //根据流水信息映射商户号
      //if (record.getGroupId == "APL") {
      if (record.getGroupId.split(",").contains("APL")) {
        if (record.getRSV4.length >= 24){
          //获取门店号
          val storeNo = record.getRSV4.substring(20, 24)
          val query_merchant_filed = record.getmId + "," + storeNo
          val query_merchant_sql = s"srcItem = \'${query_merchant_filed}\' and typeId = \'1082\'";
          val queryMerchantResult = cache$[String, SysMapItemInfo](SYS_MAP_ITEM_INFO_CACHE_NAME).get.sql(query_merchant_sql).getAll
          println("SysMapItemInfo  has " + queryResult.size() + " result by the query_merchant_sql = " + query_merchant_sql)
          if (queryMerResult.size > 0) {
            val mapId = queryMerchantResult.get(0).getValue.getMapId
            val mapResult = queryMerchantResult.get(0).getValue.getMapResult
            if (mapResult != ""){
              record.setMerNo(mapResult)
            }
          }
        }
        else println(record)
      }
      //如果都没有，用mid汇总
      if (record.getMerNo == "") {
        record.setMerNo(record.getmId())
      }
      record
    }
    val saveRecords = mapRecords.map { record =>
      //手续费计算
      //商户档案信息
      //val storeNo = record.getSerConcode.substring(20, 24)
      if (record.getFilterFlag) {
        val cfg = new CacheConfiguration(BMS_STL_INFO_CACHE_NAME)
        //      cfg.setSqlFunctionClasses(classOf[IgniteFunction])
        //todo order by decode(trim(mapp_main),'1',1,2), decode(apptype_id, 1,1,86,2,74,3,18,4,39,5,40,6,68,7)
        val query_merchant_sql = s"select * from BmsStInfo where merNo = \'${record.getMerNo}\' order by decode(trim(mappMain),\'1\',1,2), decode(apptypeId,1,1,86,2,74,3,18,4,39,5,40,6,68,7)"
        val queryMerchantResult = cache$[String, BmsStInfo](BMS_STL_INFO_CACHE_NAME).get.sql(query_merchant_sql).getAll
        println("BmsStInfo  has " + queryMerchantResult.size() + " result by the query_merchant_sql = " + query_merchant_sql)
        //当前计算手续费
        var current_charge: Double = 0
        //当前交易金额
        val current_trans_amount: Double = record.getTxnAmt

        //标志位，对应的商户号是否存在与商户结算信息表中
        if (queryMerchantResult.size > 0) {
          val creditCalcType = queryMerchantResult.get(0).getValue.getCreditCalcType
          val creditCalcRate = queryMerchantResult.get(0).getValue.getCreditCalcRate
          val creditCalcAmt = queryMerchantResult.get(0).getValue.getCreditCalcAmt
          val creditMinAmt = queryMerchantResult.get(0).getValue.getCreditMinAmt
          val creditMaxAmt = queryMerchantResult.get(0).getValue.getCreditMaxAmt
          record.setNoBmsStlInfo(false)
          if (creditCalcType == "10") {
            //按扣率计费
            current_charge = current_trans_amount / creditCalcRate
            record.setSupportedCreditCalcType(true)
          } else if (creditCalcType == "11") {
            //按笔计费
            current_charge = creditCalcAmt
            record.setSupportedCreditCalcType(true)
          } else {
            record.setSupportedCreditCalcType(false)
          }

          if (current_charge < creditMinAmt) {
            //手续费下限，不能低于该值，如果录入值为 -1 为默认值，同0
            current_charge = creditMinAmt
          } else if (current_charge > creditMaxAmt) {
            //手续费上限
            current_charge = creditMaxAmt
          }
        } else {
          record.setNoBmsStlInfo(true)
        }
        if (!record.getNoBmsStlInfo&&record.getSupportedCreditCalcType){
          //根据入账商户ID汇总可清算金额，poc没有商户id，用商户号汇总
          var today_history_amout: Double = 0.0D
          today_history_amout = cache$[String, Double](SUMMARY).get.get(record.getMerNo)
          today_history_amout = today_history_amout + current_trans_amount - current_charge
          cache$[String, Double](SUMMARY).get.put(record.getMerNo, today_history_amout)
        }
      }
      record
    }
    //saveRecords.print(50)
    saveRecords.foreachRDD { rdd =>
      rdd.foreachPartition{ iter =>
        val hbaseUtil = HbaseUtil(setting)
        val cf = "t"
        hbaseUtil.createTable(HISTORY_RECORD_TABLE, cf)
        val puts = new util.ArrayList[Put]
        while (iter.hasNext) {
          val record = iter.next()
          val now = new Date()
          val rowkey = record.getmId() + record.gettId() + now.getTime.toString
          val put = new Put(Bytes.toBytes(rowkey))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("mId"), Bytes.toBytes(record.getmId()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("tId"), Bytes.toBytes(record.gettId()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("txnAmt"), Bytes.toBytes(record.getTxnAmt()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("exchange"), Bytes.toBytes(record.getExchange()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("procCode"), Bytes.toBytes(record.getProcCode()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("respCode"), Bytes.toBytes(record.getRespCode()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("tranStat"), Bytes.toBytes(record.getTranStat()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("procCode"), Bytes.toBytes(record.getProcCode()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("serConcode"), Bytes.toBytes(record.getSerConcode()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("msgType"), Bytes.toBytes(record.getMsgType()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("filterFlag"), Bytes.toBytes(record.getFilterFlag()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("txnAmt"), Bytes.toBytes(record.getTxnAmt()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("exchange"), Bytes.toBytes(record.getExchange()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("RSV4"), Bytes.toBytes(record.getRSV4()))
          puts.add(put)
          println("add record: mid: " + record.getmId() + "tid: " + record.gettId() + "txnAmt: " + record.getTxnAmt)
        }

        val tableInterface = hbaseUtil.getConnection.getTable(TableName.valueOf(HISTORY_RECORD_TABLE))
        tableInterface.put(puts)
        tableInterface.close()
        }
    }

    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop(true, true)

  }

  def addCacheConfig(ignite: Ignite): Unit ={
    val txnCacheConfiguration = new CacheConfiguration[String, SysTxnCdInfo](SYS_TXN_CODE_INFO_CACHE_NAME)
    txnCacheConfiguration.setIndexedTypes(classOf[String], classOf[SysTxnCdInfo])
    ignite.addCacheConfiguration(txnCacheConfiguration)
    val groupCacheConfiguration = new CacheConfiguration[String, SysGroupItemInfo](SYS_GROUP_ITEM_INFO_CACHE_NAME)
    groupCacheConfiguration.setIndexedTypes(classOf[String], classOf[SysGroupItemInfo])
    ignite.addCacheConfiguration(groupCacheConfiguration)
    val mapCacheConfiguration = new CacheConfiguration[String, SysMapItemInfo](SYS_MAP_ITEM_INFO_CACHE_NAME)
    mapCacheConfiguration.setIndexedTypes(classOf[String], classOf[SysMapItemInfo])
    ignite.addCacheConfiguration(mapCacheConfiguration)
    val bmsCacheConfiguration = new CacheConfiguration[String, BmsStInfo](BMS_STL_INFO_CACHE_NAME)
    bmsCacheConfiguration.setIndexedTypes(classOf[String], classOf[BmsStInfo])
    ignite.addCacheConfiguration(bmsCacheConfiguration)
  }

}

