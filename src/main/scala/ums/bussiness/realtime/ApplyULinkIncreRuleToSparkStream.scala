package ums.bussiness.realtime

import java.util
import java.util.{Properties, Random}

import com.typesafe.config._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.scalar.scalar.{cache$, createCache$, destroyCache$}
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.codehaus.jettison.json.JSONObject
import org.json4s.native.JsonMethods.parse
import ums.bussiness.realtime.common._
import ums.bussiness.realtime.model.flow.{UlinkIncre, UlinkNormal}
import ums.bussiness.realtime.model.table._
import org.json4s.JsonAST.JObject
import org.json4s.ShortTypeHints
import org.json4s.jackson.Serialization
import org.json4s.native.JsonMethods._
import org.json4s.DefaultFormats
import java.util.Date

import org.apache.ignite.cache.CacheMode
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.spark.rdd.RDD
import ums.bussiness.realtime.common.{HbaseUtil, IgniteFunction, IgniteUtil}
import ums.bussiness.realtime.model.table.{BmsStInfo, SysGroupItemInfo, SysMapItemInfo, SysTxnCdInfo}

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

/**
  * Created by zhaikaixuan on 27/07/2017.
  */
object ApplyULinkIncreRuleToSparkStream extends Logging {
  implicit val format = Serialization.formats(ShortTypeHints(List()))
  private val CONFIG = "example-ignite.xml"
  private val cacheName = "records"
  private val SYS_TXN_CODE_INFO_CACHE_NAME = "SysTxnCdInfo"
  private val SYS_GROUP_ITEM_INFO_CACHE_NAME = "SysGroupItemInfo"
  private val SYS_MAP_ITEM_INFO_CACHE_NAME = "SysMapItemInfo"
  private val BMS_STL_INFO_CACHE_NAME = "BmsStInfo"
  private val HISTORY_RECORD_TABLE = "ulink_incremental_table"
  private val SUMMARY = "IncrementalSummary"


  def main(args: Array[String]): Unit = {
    val setting: Config = ConfigFactory.load()
    println("the answer is: " + setting.getString("simple-app.answer"))

    //read data from kafka
    val kafkaZkHost = setting.getString("kafkaZkHost")
    val appName = setting.getString("appName")
    val processInterval = setting.getInt("processInterval")
    val logLevel = setting.getString("logLevel")
    val kafkaTopics = setting.getString("kafkaTopics")
    val kafkaReceiverNum = setting.getInt("kafkaReceiverNum")
    val kafkaGroup = setting.getString("kafkaGroup")
    val kafkaThread = setting.getInt("kafkaThread")
    val master = setting.getString("sparkMaster")
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    //val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(conf)
    val streamContext = new StreamingContext(conf, Milliseconds(processInterval))
    streamContext.sparkContext.setLogLevel(logLevel)
    val sc = streamContext.sparkContext

    //初始化一个用于汇总的累加器
    //val sumMapAccum = sc.accumulator(Map[String, Double]())(HashMapAccumalatorParam)
    //    val sumMapAccum = sc.accumulator(Map[String, Double]())(HashMapAccumalatorParam)
    val sqlContext = new SQLContext(sc)
    //val topicMapUlinkIncremental = {}
    //val topicMapULinkTraditional = {}
    //val topicMap = kafkaTopics.split(",").map((_, kafkaThread)).toMap
    //ulink增量对应的topic数据读入
    val ulinkIncreTopicMap = scala.collection.immutable.Map("ULinkIncre" -> 1)
    val ulinkIncreKafkaStreams = (1 to kafkaReceiverNum).map { _ =>
      KafkaUtils.createStream(streamContext, kafkaZkHost, kafkaGroup, ulinkIncreTopicMap, StorageLevels.MEMORY_AND_DISK_SER)
    }
    val increLines = streamContext.union(ulinkIncreKafkaStreams).map(_._2)
    //ulink传统对应的topic数据读入
    /*
    val ulinkTraTopicMap = scala.collection.immutable.Map("ulink_incremental" -> 1)
    val ulinkTraKafkaStreams = (1 to kafkaReceiverNum).map { _ =>
      KafkaUtils.createStream(streamContext, kafkaZkHost, kafkaGroup, ulinkIncreTopicMap, StorageLevels.MEMORY_AND_DISK_SER)
    }
    val tradLines = streamContext.union(ulinkIncreKafkaStreams)
    */
    //导入需要找相应字段的外部表
    /*
    val (sysTxnCodeDf, sysGroupItemInfoDf, sysMapItemDf, bmsStlInfoDf) = ParseTable.parseTable(sc, sqlContext, setting)
    val sysTxnCodeDF = new QueryRelatedPropertyInDF(sqlContext, sysTxnCodeDf)
    val sysGroupItemDF = new QueryRelatedPropertyInDF(sqlContext, sysGroupItemInfoDf)
    val sysMapItemDF = new QueryRelatedPropertyInDF(sqlContext, sysMapItemDf)
    val bmsStlInfoDF = new QueryRelatedPropertyInDF(sqlContext, bmsStlInfoDf)
    */

    //val sys_group_item_info_df = sqlContext.read.parquet("table path").select("Mchnt_Id_Pay", "category")

    //read key-value from table sys_group_item_info
    /*
    val sys_group_item_info_rdd = sqlContext.read.parquet("table path").select("Mchnt_Id_Pay", "category").map(
      row => (row.getAs[String]("Mchnt_Id_Pay"), row.getAs[String]("category"))
    ).rdd

    //put key-value into cache, use igniteRDD for cache
    val igniteContext = new IgniteContext(sc, "resouces/sharedRDD.xml")
    val sharedRDD:IgniteRDD[String, String] = igniteContext.fromCache[String, String]("sys_group_item_info_rdd")
    sharedRDD.savePairs(sys_group_item_info_rdd)
    //igniteRDD for other table
    */

    //kafka setting for output
    /*
    val props: Properties = new Properties()
    props.setProperty("metadata.broker.list", "localhost:2128")
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    props.put("producer.type", "async")
    val config = new ProducerConfig(props)
    val kafkaProducer = new Producer[String, String](config)
    val sendToKafkaInc:SendMessage = new SendToKafka(kafkaProducer, "ulink_incremental")
    //val sendToKafkaTra:SendMessage = new SendToKafka(kafkaProducer, "ulink_traditional")
    */

    //hbase initialization
    /*
    val hbaseUtils = HbaseUtilCp("172.17.1.146:2128")
    val hbaseUtils = HbaseUtilCp(setting)
    val hbaseUtils = HbaseUtil(setting)
    val summaryName = "summary"
    hbaseUtils.createTable(summaryName)
    */
    //无需从远程服务器上pull，rule包直接放置在本地
    /*
    val ks = KieServices.Factory.get()
    val kieContainer = ks.newKieClasspathContainer()
    val kieSession = kieContainer.newKieSession()
    */
    //apply drools rules to each item in rdd
    /*
    val ks = KieServices.Factory.get()
    val releaseId = ks.newReleaseId("com.ums", "ruleEngine", "1.0.0")
    val kieContainer = ks.newKieContainer(releaseId)
    val scanner = ks.newKieScanner(kieContainer)
    //val broadcastKieContainer = sc.broadcast(kieContainer)
    val broadcastKieScanner = sc.broadcast(scanner)
    broadcastKieScanner.value.start(10000L)
    val kieSession = kieContainer.newKieSession("filterSession")
    */
    //kieSession.setGlobal("producer", sendToKafkaInc)

    //val queryServiceImp:QueryRelatedProperty = new QueryRelatedPropertyInDF(sys_group_item_info_df)
    //kieSession.setGlobal("queryService", queryServiceImp)
    //val kSession = sc.broadcast(kieSession)
    IgniteUtil(setting)
    destroyCache$(SUMMARY)
    createCache$(SUMMARY, indexedTypes = Seq(classOf[String], classOf[Double]))
    val records = increLines.map {
      line => {
        val ulinkIncre = new UlinkIncre()
        //println(line)
        //val formatLine = line.replaceAll("\":null\"", ":\"\"")
        try {
          //implicit val formats = DefaultFormats
          val JObject(message) = parse(line).asInstanceOf[JObject]
          message.map(keyValue =>
            keyValue._1 match {
              case "PLT_SSN" => ulinkIncre.setPltSsn(keyValue._2.extract[String])
              case "TRANS_CD_PAY" => ulinkIncre.setTransCdPay(keyValue._2.extract[String])
              case "PAY_ST" => ulinkIncre.setPaySt(keyValue._2.extract[String])
              case "TRANS_ST_RSVL" => ulinkIncre.setTransStRsvl(keyValue._2.extract[String])
              case "ROUT_INST_ID_CD" => ulinkIncre.setRoutInstIdCd(keyValue._2.extract[String])
              case "TRANS_ST" => ulinkIncre.setTransSt(keyValue._2.extract[String])
              case "TRANS_ST_RSVL" => ulinkIncre.setTransStRsvl(keyValue._2.extract[String])
              case "PROD_STYLE" => ulinkIncre.setProdStyle(keyValue._2.extract[String])
              case "ROUT_INST_ID_CD" => ulinkIncre.setRoutInstIdCd(keyValue._2.extract[String])
              case "MCHNT_ID_PAY" => ulinkIncre.setMchntIdPay(keyValue._2.extract[String])
              case "TERM_ID_PAY" => ulinkIncre.setTermIdPay(keyValue._2.extract[String])
              case "TRANS_CD_PAY" => ulinkIncre.setTransCdPay(keyValue._2.extract[String])
              case "PAY_ST" => ulinkIncre.setPaySt(keyValue._2.extract[String])
              case "TRANS_AMT" => ulinkIncre.setTransAmt(keyValue._2.extract[String].toDouble)
              case "RSVD1" => ulinkIncre.setdRsvd1(keyValue._2.extract[String])
              case "RSVD6" => ulinkIncre.setdRsvd6(keyValue._2.extract[String].toInt)
              case _ =>
            }
          )
        } catch {
          case e: Exception => logError("Parse Json ERROR: " + line)
        }
        ulinkIncre
      }
    }
    val filterRecords = records.mapPartitions {
      val filterRecords = new ArrayBuffer[UlinkIncre]
      iter => {
        val randomString = new Random(10).longs().toString
        val cacheName = "increRecords"
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
        val sql = "(UlinkIncre.transCdPay = \'02S221X1\' or UlinkIncre.transCdPay = \'02V523X1\'" +
          " or UlinkIncre.transCdPay = \'07S30609\') and UlinkIncre.paySt = \'0\' and UlinkIncre.transStRsvl = \'0\' " +
          "and UlinkIncre.routInstIdCd = \'3748020000\' and UlinkIncre.transSt = \'0\' " +
          "and UlinkIncre.prodStyle = \'51A2\' and UlinkIncre.dRsvd6 = \'1001\'"
        val result = cache$[String, UlinkIncre](cacheName).get.sql(sql).getAll
        val result_iterator = result.iterator()
        while (result_iterator.hasNext) {
          val record = result_iterator.next()
          filterRecords += record.getValue
        }
        println("after filtering in partition:" + result.size())

        cache$(cacheName).get.destroy()
        filterRecords.iterator
      }
    }
    filterRecords.print(50)
    //交易码转换
    val transRecords = filterRecords.map { record =>
      val ignite = IgniteUtil(setting)
      addCacheConfig(ignite)
      val transcd = record.getTransCdPay
      val query_sql = "SysTxnCdInfo.txnKey = \'" + transcd + "\'"
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
    filterRecords.print(10)
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
      var query_mer_filed = record.getMchntIdPay + "," + record.getTermIdPay
      var query_mer_sql = s"SysMapItemInfo.srcItem = \'${query_mer_filed}\' and SysMapItemInfo.typeId = \'1\'";
      var queryMerResult = cache$[String, SysMapItemInfo](SYS_MAP_ITEM_INFO_CACHE_NAME).get.sql(query_mer_sql).getAll
      println("SysMapItemInfo  has " + queryMerResult.size() + " result by the query_mer_sql = " + query_mer_sql)
      if (queryMerResult.size > 0) {
        val mapId = queryMerResult.get(0).getValue.getMapId
        val mapResult = queryMerResult.get(0).getValue.getMapResult
        if (mapResult != ""){
          record.setMerNo(mapResult)
        }
      }

      //根据流水信息映射商户号
      //if (record.getGroupId == "APL") {
      if (record.getdRsvd1().length >= 3){
        if (record.getdRsvd1.substring(0, 3) == "SML")
        {
          //获取门店号
          val storeNo = record.getdRsvd1().substring(7, 50)
          val query_merchant_sql = s"srcItem = \'${storeNo}\' and typeId = \'1068\'";
          val queryMerchantResult = cache$[String, SysMapItemInfo](SYS_MAP_ITEM_INFO_CACHE_NAME).get.sql(query_merchant_sql).getAll
          println("SysMapItemInfo  has " + queryResult.size() + " result by the query_merchant_sql = " + query_merchant_sql)
          if (queryMerResult.size > 0) {
            val mapId = queryMerchantResult.get(0).getValue.getMapId
            val mapResult = queryMerchantResult.get(0).getValue.getMapResult
            if (mapResult != "") {
              record.setMerNo(mapResult)
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
        //当前计算手续费
        var current_charge: Double = 0
        //当前交易金额
        val current_trans_amount: Double = record.getTransAmt
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
/*
    val incrementalResult = increLines.transform{
      rdd => rdd.mapPartitions {
        partition => {
          val newPartition = partition.
      
/*filter {
            //假设输入的数据的每行交易清单是符合以json字符串格式的 string
            item =>
              val JItem = new JSONObject(item.toString())
              val itemAfterParsing = new UlinkIncre(JItem.getString("TRANS_CD_PAY"),
                JItem.getString("PAY_ST"),
                JItem.getString("TRANS_ST_RSVL"),
                JItem.getString("ROUT_INST_ID_CD"),
                JItem.getString("TRANS_ST"),
                JItem.getString("PROD_STYLE"),
                JItem.getInt("RSVD6"),
                JItem.getString("MCHNT_ID_PAY"),
                JItem.getString("TERM_ID_PAY"),
                false,
                0,
                0)

              //var extendedItem = extendProperty(itemAfterParsing)
              // val cacheRDD = igniteContext.fromCache("sys_group_item_info_rdd")
              //val result = cacheRDD.sql("select ")
              //itemAfterParsing
              //val message = new KeyedMessage[String, String]("topic", "message")
              //kafkaProducer.send(message) //moved to rules
              kSession.value.insert(itemAfterParsing)
              kSession.value.fireAllRules()
              itemAfterParsing.getFilterFlag
          }.*/
map {
            item =>
              val JItem = new JSONObject(item.toString())
              val itemAfterParsing = new UlinkIncre(JItem.getString("TRANS_CD_PAY"),
                JItem.getString("PAY_ST"),
                JItem.getString("TRANS_ST_RSVL"),
                JItem.getString("ROUT_INST_ID_CD"),
                JItem.getString("TRANS_ST"),
                JItem.getString("PROD_STYLE"),
                JItem.getInt("RSVD6"),
                JItem.getString("MCHNT_ID_PAY"),
                JItem.getString("TERM_ID_PAY"),
                false,
                0,
                0)
              //parsing
              //query properties
              //根据TRANS_CD_PAY去SYS_TXN_CODE_INFO表中找到相应的是否纳入清算
              val settleFlag = sysTxnCodeDF.queryProperty("settleFlag", "txn_key", JItem.getString("TRANS_CD_PAY"), "=")
              itemAfterParsing.setClearingFlag(settleFlag)
              //根据TRANS_CD_PAY去SYS_TXN_CODE_INFO表中找到相应的借贷
              val dcFlag = sysTxnCodeDF.queryProperty("dcFlag", "txn_key", JItem.getString("TRANS_CD_PAY"), "=")
              itemAfterParsing.setDcFlag(dcFlag.toInt)
              //根据 Mchnt_Id_Pay 字段去 sys_group_item_info 里获取分组信息
              val groupId = sysGroupItemDF.queryProperty("groupId", "item", JItem.getString("Mchnt_Id_Pay"), "=")
              itemAfterParsing.setGroupId(groupId)
              //TODO,sys_map_item_info的表结构,商户编号对应于哪个字段
              //源字段为 Mchnt_Id_Pay+ Term_Id_Pay，根据源字段去清分映射表 sys_map_item_info 中查找结果字段，并将结果字段作为入账商户编号
              val merNo: String ={
                val merNoByTer = sysMapItemDF.queryMerNo("map_result", "src_item", JItem.getString("MCHNT_ID_PAY")+JItem.getString("TERM_ID_PAY"), "=", 1)
                val merNoByUlink = if (JItem.getString("RSVD1").substring(0,3).equals("SML")){
                  sysMapItemDF.queryMerNo("map_result", "src_item", JItem.getString("RSVD1").substring(50, 57), "=", 1068)
                }else{
                  ""
                }
                val merNoByQuery=if (merNoByUlink.equals("")){
                  merNoByTer
                }else{
                  merNoByUlink
                }
                if(merNoByQuery.equals("")){
                  JItem.getString("MCHNT_ID_PAY")
                }else{
                  merNoByQuery
                }
              }
              /*            val merNo = sysMapItemDF.queryMerNo("map_result", "src_item", JItem.getString("MCHNT_ID_PAY")+JItem.getString("TERM_ID_PAY"), "=", 1)
                          if (JItem.getString("RSVD1").substring(0,3).equals("SML")){
                            val merNo = sysMapItemDF.queryMerNo("map_result", "src_item", JItem.getString("RSVD1").substring(50, 57), "=", 1068)
                          }*/

              itemAfterParsing.setMerNo(merNo)
              //TODO,do not need merid anymore
              /*            val merId = bmsStlInfoDF.queryProperty("mer_id", "mer_no", merNo, "=")
                          itemAfterParsing.setMerId(merId.toInt)*/
              //查看交易金额
              itemAfterParsing.setTransAmt(JItem.getDouble("TRANS_AMT"))
              //val jo = new JSONObject(itemAfterParsing)
              //jo.toString
              itemAfterParsing

          }.filter {   //根据清算标志位判断是否纳入清算
            item =>
              clearFlagList.contains(item.getClearingFlag)
          }.map {   //计算手续费，
            //TODO, 按商户编号汇总
            item =>
              val creditMinAmt = bmsStlInfoDF.queryPropertyInBMS_STL_INFODF("credit_min_amt", "mer_no", item.getMerNo).getOrElse("-1")
              val creditMaxAmt = bmsStlInfoDF.queryPropertyInBMS_STL_INFODF("creditMaxAmt", "mer_no", item.getMerNo).getOrElse("-1")
              bmsStlInfoDF.queryPropertyInBMS_STL_INFODF("credit_calc_type", "mer_no", item.getMerNo) match {
                case Some(calType) => {
                  if (calType == "10"){
                    val creditCalcRate = bmsStlInfoDF.queryPropertyInBMS_STL_INFODF("credit_calc_rate", "mer_no", item.getMerNo).getOrElse("-1")
                    if (creditCalcRate.toDouble * item.getTransAmt < creditMinAmt.toDouble) item.setExchange(creditMinAmt.toDouble)
                    else if (creditCalcRate.toDouble * item.getTransAmt > creditMaxAmt.toDouble) item.setExchange(creditMaxAmt.toDouble)
                    else item.setExchange(creditCalcRate.toDouble * item.getTransAmt)
                  }

                }
                case Some("11") => {
                  val creditCalcAmt = bmsStlInfoDF.queryPropertyInBMS_STL_INFODF("credit_calc_amt", "mer_no", item.getMerNo).getOrElse("-1")
                  if (creditCalcAmt < creditMinAmt) item.setExchange(creditMinAmt.toDouble)
                  else if (creditCalcAmt > creditMaxAmt) item.setExchange(creditMaxAmt.toDouble)
                  else item.setExchange(creditCalcAmt.toDouble)
                }
                case None => {
                  item.setNoBmsStlInfo(true)
                  item.setExchange(0)
                }
              }
              //sumMapAccum += Map(item.getMerId.toString -> (item.getTransAmt - item.getExchange))
              //            sumMapAccum += Map(item.getMerId.toString -> (item.getTransAmt - item.getExchange))
              item
          }.toList
          newPartition.iterator
        }
      }
    }
    //保存汇总信息到HBase
    /*
    val columnName = "sum"
    for ((k, v) <- sumMapAccum.value){
      hbaseUtils.writeTable(summaryName, k, columnName, v.toString)
    }
    */
    //    val columnName = "sum"
    //    for ((k, v) <- sumMapAccum.value){
          //hbaseUtils.writeTable(summaryName, k, columnName, v.toString)
        // hbaseUtils.writeTable(summaryName, k, columnName, v.toString)
         //hbaseUtils.writeTable(summaryName, k, columnName, v.toString)

    //    }
    //sumMapAccum.toString()
    //incrementalResult.saveAsObjectFiles("ums_poc", ".obj")
    //汇总
    val sum = Map[String, Double]()
    incrementalResult.foreachRDD{
      rdd =>
        val sumMapAccum = sc.accumulator(Map[String, Double]())(HashMapAccumalatorParam)
        rdd.map{
          item =>
            sumMapAccum += Map(item.getMerId.toString -> (item.getTransAmt - item.getExchange))
      }
        sum ++= sumMapAccum.value.map{ case (k, v) => k -> (v + sum.getOrElse(k, 0.toDouble))}
    }

    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop(true, true)

  }

  //def extendProperty(propertyValueMatched:String, propertyNameMatched:String, extendProperties:List[String], targetCacheRDD:IgniteRDD[])
}
*/