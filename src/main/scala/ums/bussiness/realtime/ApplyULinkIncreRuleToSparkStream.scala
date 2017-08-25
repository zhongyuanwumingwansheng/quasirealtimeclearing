package ums.bussiness.realtime

import java.util
import java.util.Date

import com.typesafe.config._
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}
import org.json4s.JsonAST.JObject
import org.json4s.ShortTypeHints
import org.json4s.jackson.Serialization
import org.json4s.native.JsonMethods._
import ums.bussiness.realtime.model.flow.UlinkIncre
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.Ignite
import ums.bussiness.realtime.common.{HbaseUtil, IgniteUtil}
import ums.bussiness.realtime.model.table.{BmsStInfo, SysGroupItemInfo, SysMapItemInfo, SysTxnCdInfo}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaikaixuan on 27/07/2017.
  */
object ApplyULinkIncreRuleToSparkStream extends Logging {
  implicit val format = Serialization.formats(ShortTypeHints(List()))
  private val SYS_TXN_CODE_INFO_CACHE_NAME = "SysTxnCdInfo"
  private val SYS_GROUP_ITEM_INFO_CACHE_NAME = "SysGroupItemInfo"
  private val SYS_MAP_ITEM_INFO_CACHE_NAME = "SysMapItemInfo"
  private val BMS_STL_INFO_CACHE_NAME = "BmsStInfo"
  private val HISTORY_RECORD_TABLE = "ulink_incremental_table"
  private val SUMMARY = "IncrementalSummary"


  def main(args: Array[String]): Unit = {
    val setting: Config = ConfigFactory.load()
    val kafkaZkHost = setting.getString("kafkaZkHost")
    val appName = setting.getString("normalAppName")
    val processInterval = setting.getInt("processInterval")
    val logLevel = setting.getString("logLevel")
    val kafkaTopics = setting.getString("kafkaTopics")
    val kafkaReceiverNum = setting.getInt("kafkaReceiverNum")
    val kafkaGroup = setting.getString("kafkaGroup")
    val kafkaThread = setting.getInt("kafkaThread")
    val master = setting.getString("sparkMaster")
    val brokers = setting.getString("kafkaHost")
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.streaming.backpressure.enabled","true")
    conf.set("spark.streaming.kafka.maxRatePerPartition",setting.getString("maxRatePerPartition"))
    val streamContext = new StreamingContext(conf, Milliseconds(processInterval))
    streamContext.sparkContext.setLogLevel(logLevel)
    val sc = streamContext.sparkContext
    sc.setLogLevel("ERROR")
    val topicsSet: Set[String] = Set[String]("ULinkIncre")
    //val topicsSet: Set[String] = kafkaTopics.split("ULinkIncre").toSet
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> kafkaGroup,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "auto.offset.reset" -> "smallest" //自动将偏移重置为最早的偏移
    )
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamContext, kafkaParams, topicsSet).flatMap(line => Some(line._2.toString()))

    IgniteUtil(setting)

    destroyCache$(SUMMARY)
    createCache$(SUMMARY, indexedTypes = Seq(classOf[String], classOf[Double]))

    val records = lines.map {
      line => {
        val ulinkIncre = new UlinkIncre()
        try {
          val formatLine = line.replaceAll("\":null\"", ":\"\"")
          val JObject(message) = parse(formatLine).asInstanceOf[JObject]
          message.map(keyValue =>
            keyValue._1 match {
              case "PLT_SSN" => ulinkIncre.setPltSsn(keyValue._2.extract[String])
              case "TRANS_ST" => ulinkIncre.setTransSt(keyValue._2.extract[String])
              case "TRANS_ST_RSVL" => ulinkIncre.setTransStRsvl(keyValue._2.extract[String])
              case "PROD_STYLE" => ulinkIncre.setProdStyle(keyValue._2.extract[String])
              case "ROUT_INST_ID_CD" => ulinkIncre.setRoutInstIdCd(keyValue._2.extract[String])
              case "MCHNT_ID_PAY" => ulinkIncre.setMchntIdPay(keyValue._2.extract[String])
              case "TERM_ID_PAY" => ulinkIncre.setTermIdPay(keyValue._2.extract[String])
              case "TRANS_CD_PAY" => ulinkIncre.setTransCdPay(keyValue._2.extract[String])
              case "PAY_ST" => ulinkIncre.setPaySt(keyValue._2.extract[String])
              case "TRANS_AMT" => {
                if (keyValue._2.extract[String] != ""){
                  ulinkIncre.setTransAmt(keyValue._2.extract[String].toDouble)
                }
              }
              case "RSVD1" => ulinkIncre.setdRsvd1(keyValue._2.extract[String])
              case "RSVD6" => {
                if (keyValue._2.extract[String] != ""){
                  ulinkIncre.setdRsvd6(keyValue._2.extract[String].toInt)
                }
              }
              case _ =>
            }
          )
        } catch {
          case e: Exception => logError("Parse Json ERROR: " + line)
        }
        ulinkIncre
      }
    }
    //records.print()
    val filterRecords = records.mapPartitions {
      val filterRecords = new ArrayBuffer[UlinkIncre]

      iter => {
        val randomString = Math.random().toString
        val cacheName = "increRecords" + randomString
        //+ new Random(10).longs().toString
        //println("before filtering in partition:" + iter.length)
        val ignite = IgniteUtil(setting)
        destroyCache$(cacheName)
        createCache$(cacheName, CacheMode.LOCAL, indexedTypes = Seq(classOf[String], classOf[UlinkIncre]))
        iter.foreach { record =>
          cache$(cacheName).get.put(record.getPltSsn() + record.getMchntIdPay + record.getTermIdPay, record)
          //println(record.toString)
        }
        //        val sql = "(UlinkNormal.procCode != 01 and UlinkNormal.procCode != 02)"
        val sql = "((UlinkIncre.transCdPay = \'02S221X1\' or UlinkIncre.transCdPay = \'02V523X1\'" +
          " or UlinkIncre.transCdPay = \'07S30609\') and UlinkIncre.paySt = \'0\' and UlinkIncre.transStRsvl = \'0\' " +
          "and UlinkIncre.routInstIdCd = \'3748020000\' and UlinkIncre.transSt = \'0\' " +
          "and UlinkIncre.prodStyle = \'51A2\' and UlinkIncre.dRsvd6 = \'1001\') or" +
          "(UlinkIncre.transSt = \'0\' and UlinkIncre.transStRsvl = \'0\' and UlinkIncre.paySt = \'0\'" +
          "and UlinkIncre.prodStyle = \'51A2\' and (UlinkIncre.transCdPay = \'02S221X0\' or UlinkIncre.transCdPay = \'02V523X0\' or UlinkIncre.transCdPay = \'02R222X0\'" +
          "or (UlinkIncre.transCdPay = \'07S30609\' and UlinkIncre.dRsvd6 = \'1002\')))"
        val result = cache$[String, UlinkIncre](cacheName).get.sql(sql).getAll
        val result_iterator = result.iterator()
        while (result_iterator.hasNext) {
          val record = result_iterator.next()
          filterRecords += record.getValue
        }
        cache$(cacheName).get.destroy()
        filterRecords.iterator
      }
    }
    //filterRecords.print(50)
    //交易码转换
    val transRecords = filterRecords.map { record =>
      val ignite = IgniteUtil(setting)
      addCacheConfig(ignite)
      val transcd = record.getTransCdPay
      val query_sql = s"SysTxnCdInfo.txnKey = \'${transcd}\'"
      val queryResult = cache$[String, SysTxnCdInfo](SYS_TXN_CODE_INFO_CACHE_NAME).get.sql(query_sql).getAll
      println("SysTxnCdInfo  has " + queryResult.size() + " result by the txnKey = " + transcd)
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

    val mapRecords = transRecords.map { record =>
      //清分规则 ID 获取,可能不止一个，所以通过逗号进行拼接
      val query_group_sql = s"SysGroupItemInfo.item = \'${record.getMchntIdPay}\'";
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
      val query_mer_filed = record.getMchntIdPay + "," + record.getTermIdPay
      val query_mer_sql = s"SysMapItemInfo.srcItem = \'${query_mer_filed}\' and SysMapItemInfo.typeId = \'1\'";
      val queryMerResult = cache$[String, SysMapItemInfo](SYS_MAP_ITEM_INFO_CACHE_NAME).get.sql(query_mer_sql).getAll
      println("SysMapItemInfo  has " + queryMerResult.size() + " result by the query_mer_sql = " + query_mer_sql)
      if (queryMerResult.size > 0) {
        val mapId = queryMerResult.get(0).getValue.getMapId
        val mapResult = queryMerResult.get(0).getValue.getMapResult
        if (mapResult != ""){
          record.setMerNo(mapResult)
          record.setMapResultFromTerminal(mapResult)
        }
      }

      //根据流水信息映射商户号
      //if (record.getGroupId == "APL") {
      if (record.getdRsvd1().length >= 3){
        if (record.getdRsvd1.substring(0, 3) == "SML")
        {
          //获取门店号
          val storeNo = record.getdRsvd1().substring(7, 50)
          record.setStoreNo(storeNo)
          val query_merchant_sql = s"srcItem = \'${storeNo}\' and typeId = \'1068\'";
          val queryMerchantResult = cache$[String, SysMapItemInfo](SYS_MAP_ITEM_INFO_CACHE_NAME).get.sql(query_merchant_sql).getAll
          println("SysMapItemInfo  has " + queryResult.size() + " result by the query_merchant_sql = " + query_merchant_sql)
          if (queryMerResult.size > 0) {
            val mapId = queryMerchantResult.get(0).getValue.getMapId
            val mapResult = queryMerchantResult.get(0).getValue.getMapResult
            if (mapResult != "") {
              record.setMerNo(mapResult)
              record.setMapResultFromRSV(mapResult)
            }
          }
        }
      }
      //如果都没有，用MchntIdPay汇总
      if (record.getMerNo == "") {
        record.setMerNo(record.getMchntIdPay)
      }
      record
    }
    mapRecords.print()
    val saveRecords = mapRecords.map { record =>
      //手续费计算
      //商户档案信息
      //val storeNo = record.getSerConcode.substring(20, 24)
      if (record.getFilterFlag) {
//        val cfg = new CacheConfiguration(BMS_STL_INFO_CACHE_NAME)
        //      cfg.setSqlFunctionClasses(classOf[IgniteFunction])
        //todo order by decode(trim(mapp_main),'1',1,2), decode(apptype_id, 1,1,86,2,74,3,18,4,39,5,40,6,68,7)
        val query_merchant_sql = s"select * from BmsStInfo where merNo = \'${record.getMchntIdPay}\' order by decode(trim(mappMain),\'1\',1,2), decode(apptypeId,1,1,86,2,74,3,18,4,39,5,40,6,68,7)"
        val queryMerchantResult = cache$[String, BmsStInfo](BMS_STL_INFO_CACHE_NAME).get.sql(query_merchant_sql).getAll
        //当前计算手续费
        var current_charge: Double = 0
        //当前交易金额, ulink增值条件下对应的transAmt单位为分，转化为元
        val current_trans_amount: Double = record.getTransAmt/100D
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
            current_charge = current_trans_amount * creditCalcRate / 100D
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
          if (record.getDcFlag == 1){ //1为借记，负交易
            //根据入账商户ID汇总可清算金额，poc没有商户id，用商户号汇总
            var today_history_amout: Double = 0.0D
            today_history_amout = cache$[String, Double](SUMMARY).get.get(record.getMerNo)

            today_history_amout = today_history_amout - current_trans_amount + current_charge
            cache$[String, Double](SUMMARY).get.put(record.getMerNo, today_history_amout)
            println("cache item " + record.getMerNo)
          }
          else if (record.getDcFlag == -1){ //-1为借记，正交易
            //根据入账商户ID汇总可清算金额，poc没有商户id，用商户号汇总
            var today_history_amout: Double = 0.0D
            today_history_amout = cache$[String, Double](SUMMARY).get.get(record.getMerNo)
            today_history_amout = today_history_amout + current_trans_amount - current_charge
            cache$[String, Double](SUMMARY).get.put(record.getMerNo, today_history_amout)
            println("cache item " + record.getMerNo)
          }
          else {
            println("dcFlag error is not 1|-1: " + record)
          }
        }
      }
      record
    }
    //saveRecords.print()
    saveRecords.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        val hbaseUtil = HbaseUtil(setting)
        val cf = "t"
        hbaseUtil.createTable(HISTORY_RECORD_TABLE, cf)
        val puts = new util.ArrayList[Put]
        while (iter.hasNext) {
          val record = iter.next()
          val now = new Date()
          val rowkey = record.getMchntIdPay() + record.getTermIdPay() + now.getTime.toString
          val put = new Put(Bytes.toBytes(rowkey))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("mchntIdPay"), Bytes.toBytes(record.getMchntIdPay()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("transCdPay"), Bytes.toBytes(record.getTransCdPay()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("paySt"), Bytes.toBytes(record.getPaySt()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("transStRsvl"), Bytes.toBytes(record.getTransStRsvl()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("routInstIdCd"), Bytes.toBytes(record.getRoutInstIdCd()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("transSt"), Bytes.toBytes(record.getTransSt()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("termIdPay"), Bytes.toBytes(record.getTermIdPay()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("filterFlag"), Bytes.toBytes(record.getFilterFlag()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("prodStyle"), Bytes.toBytes(record.getProdStyle()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("dRsvd6"), Bytes.toBytes(record.getdRsvd6()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("transAmt"), Bytes.toBytes(record.getTransAmt()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("exchange"), Bytes.toBytes(record.getExchange()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("dcFlag"), Bytes.toBytes(record.getDcFlag()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("merNo"), Bytes.toBytes(record.getMerNo()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("groupId"), Bytes.toBytes(record.getGroupId()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("storeNo"), Bytes.toBytes(record.getStoreNo()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("mapResultFromTerminal"), Bytes.toBytes(record.getMapResultFromTerminal()))
          put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("mapResultFromRSV"), Bytes.toBytes(record.getMapResultFromRSV()))
          puts.add(put)
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
