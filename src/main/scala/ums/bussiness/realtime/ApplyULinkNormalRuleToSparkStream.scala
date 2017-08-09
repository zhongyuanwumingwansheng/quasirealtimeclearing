package ums.bussiness.realtime

import java.util.Properties

import org.apache.spark.Logging
import com.typesafe.config._
import kafka.javaapi.producer.Producer
import kafka.producer.ProducerConfig
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}
import org.codehaus.jettison.json.JSONObject
import org.kie.api.builder.KieBuilder
import org.kie.api.builder.KieScanner
import org.kie.api.builder.ReleaseId
import org.kie.api.runtime.KieContainer
import org.kie.api.runtime.KieSession
import ums.bussiness.realtime.common.ParseTable
//import org.json._
import org.kie.api.KieServices
import ums.bussiness.realtime.common.{HbaseUtil, QueryRelatedPropertyInDF, SendMessage, SendToKafka}
import ums.bussiness.realtime.model.flow.UlinkNormal
//import com.ums.HashMapAccumalatorParam
import scala.collection.mutable.Map
//import com.ums.HashMapAccumalatorParam
//import org.json4s._
//import org.json4s.native.JsonMethods._
/**
  * Created by zhaikaixuan on 27/07/2017.
  */
object ApplyULinkNormalRuleToSparkStream extends Logging{
  def main(args: Array[String]): Unit = {
    val setting:Config = ConfigFactory.load()
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
    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    //val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(conf)
    val streamContext = new StreamingContext(conf, Milliseconds(processInterval))
    streamContext.sparkContext.setLogLevel(logLevel)
    val sc = streamContext.sparkContext

    //初始化一个用于汇总的累加器
//    val sumMapAccum = sc.accumulator(Map[String, Double]())(HashMapAccumalatorParam)
    val sqlContext = new SQLContext(sc)
    //val topicMapUlinkIncremental = {}
    //val topicMapULinkTraditional = {}
    val topicMap = kafkaTopics.split(",").map((_, kafkaThread)).toMap
    val kafkaStreams = (1 to kafkaReceiverNum).map { _ =>
      KafkaUtils.createStream(streamContext, kafkaZkHost, kafkaGroup, topicMap, StorageLevels.MEMORY_AND_DISK_SER)
    }
    val lines = streamContext.union(kafkaStreams)
    //导入需要找相应字段的外部表
    val (sysTxnCodeDf, sysGroupItemInfoDf, sysMapItemDf, bmsStlInfoDf) = ParseTable.parseTable(sc, sqlContext, setting)
    val sysTxnCodeDF = new QueryRelatedPropertyInDF(sqlContext, sysTxnCodeDf)
    val sysGroupItemDF = new QueryRelatedPropertyInDF(sqlContext, sysGroupItemInfoDf)
    val sysMapItemDF = new QueryRelatedPropertyInDF(sqlContext, sysMapItemDf)
    val bmsStlInfoDF = new QueryRelatedPropertyInDF(sqlContext, bmsStlInfoDf)

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
    val props: Properties = new Properties()
    props.setProperty("metadata.broker.list", "localhost:2128")
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    props.put("producer.type", "async")
    val config = new ProducerConfig(props)
    val kafkaProducer = new Producer[String, String](config)
    val sendToKafkaInc:SendMessage = new SendToKafka(kafkaProducer, "ulink_normal")
    //val sendToKafkaTra:SendMessage = new SendToKafka(kafkaProducer, "ulink_traditional")

    //hbase initialization
    val hbaseUtils = HbaseUtil(setting)
    val summaryName = "summary"
    hbaseUtils.createTable(summaryName)

    //apply drools rules to each item in rdd
    val ks = KieServices.Factory.get()
    val releaseId = ks.newReleaseId("com.ums", "ruleEngine", "1.0.0")
    val kieContainer = ks.newKieContainer(releaseId)
    val scanner = ks.newKieScanner(kieContainer)
    //val broadcastKieContainer = sc.broadcast(kieContainer)
    val broadcastKieScanner = sc.broadcast(scanner)
    broadcastKieScanner.value.start(10000L)
    val kieSession = kieContainer.newKieSession("filterSession")
    kieSession.setGlobal("producer", sendToKafkaInc)

    //val queryServiceImp:QueryRelatedProperty = new QueryRelatedPropertyInDF(sys_group_item_info_df)
    //kieSession.setGlobal("queryService", queryServiceImp)
    val kSession = sc.broadcast(kieSession)
    val clearFlagList = List("1", "2", "3", "4", "5", "6")

    /*
    val items = lines.foreachRDD{
      DiscretizedRdd =>
        DiscretizedRdd.foreachPartition{
    */
    val result = lines.transform{
      rdd => rdd.mapPartitions {
        partition => {
          val newPartition = partition.filter {
            //假设输入的数据的每行交易清单是符合以json字符串格式的 string
            item =>
              val JItem = new JSONObject(item.toString())
              val itemAfterParsing = new UlinkNormal(JItem.getString("PROC_CODE"),
                JItem.getString("RESP_CODE"),
                JItem.getString("TRAN_STATUS"),
                JItem.getString("MID"),
                JItem.getString("TID"),
                JItem.getString("MSG_TYPE"),
                JItem.getString("SER_CONCODE"),
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
          }.map {
            item =>
              val JItem = new JSONObject(item.toString())
              val itemAfterParsing = new UlinkNormal(JItem.getString("PROC_CODE"),
                JItem.getString("RESP_CODE"),
                JItem.getString("TRAN_STATUS"),
                JItem.getString("MID"),
                JItem.getString("TID"),
                JItem.getString("MSG_TYPE"),
                JItem.getString("SER_CONCODE"),
                false,
                0,
                0)
              //parsing
              //query properties
              //TODO,settleFlag,dcFlag
              //根据MSG_TYPE[1,2]|PROC_CODE[0,1]| SER_CONCODE这3个字段，
              // 与SYS_TXN_CODE_INFO表中txn_key 字段的中第一个字段的
              //2-3,第二个0-1,第三个匹配,判断是否纳入清算
              val txnKey=s"%${JItem.getString("TMSG_TYPE").substring(1,3)}%${JItem.getString("PROC_CODE").substring(0,2)}%${JItem.getString("SER_CONCODE")}%"
              val settleFlag = sysTxnCodeDF.queryProperty("settleFlag", "txn_key", txnKey, "like")
              itemAfterParsing.setClearingFlag(settleFlag)
              //同上,去SYS_TXN_CODE_INFO表中找到相应的借贷
              val dcFlag = sysTxnCodeDF.queryProperty("dcFlag", "txn_key", JItem.getString("TRANS_CD_PAY"), "=")
              itemAfterParsing.setDcFlag(dcFlag.toInt)
              //根据 Mchnt_Id_Pay 字段去 sys_group_item_info 里获取分组信息
              val groupId = sysGroupItemDF.queryProperty("groupId", "item", JItem.getString("MID"), "=")
              itemAfterParsing.setGroupId(groupId)
              //TODO,sys_map_item_info的表结构,商户编号对应于哪个字段
              //源字段为 MID+ TID，根据源字段去清分映射表 sys_map_item_info 中查找结果字段，并将结果字段作为入账商户编号
              val merNo: String ={
                val merNoByTer = sysMapItemDF.queryMerNo("map_result", "src_item", JItem.getString("MID")+JItem.getString("TID"), "=", 1)
                val merNoByUlink = if (groupId.equals("APL")){
                  sysMapItemDF.queryMerNo("map_result", "src_item", JItem.getString("MID")+JItem.getString("RSV4").substring(24, 44), "=", 1082)
                }else{
                  ""
                }
                val merNoByQuery=if (merNoByUlink.equals("")){
                  merNoByTer
                }else{
                  merNoByUlink
                }
                if(merNoByQuery.equals("")){
                  JItem.getString("MID")
                }else{
                  merNoByQuery
                }
              }
              itemAfterParsing.setMerNo(merNo)
              /*            val merNo = sysMapItemDF.queryMerNo("map_result", "src_item", JItem.getString("MID")+JItem.getString("TID"), "=", 1)
                          //TODO,两种获取商户编号的方法，什么时候用哪种如何判断
                          if (groupId.equals("APL")){
                              val merNo = sysMapItemDF.queryMerNo("map_result", "src_item", JItem.getString("MID")+JItem.getString("RSV4").substring(24, 44), "=", 1082)
                          }*/
              //TODO,do not need merid anymore
              /*            val merId = bmsStlInfoDF.queryProperty("mer_id", "mer_no", merNo, "=")
                          itemAfterParsing.setMerId(merId.toInt)*/
              //查看交易金额
              itemAfterParsing.setTxnAmt(JItem.getDouble("TRANS_AMT"))
              //val jo = new JSONObject(itemAfterParsing)
              //jo.toString
              itemAfterParsing

          }.filter {   //根据清算标志位判断是否纳入清算
            item =>
              clearFlagList.contains(item.getClearingFlag)
          }.map {   //计算手续费
            // TODO, 按商户编号汇总
            item =>
              val creditMinAmt = bmsStlInfoDF.queryPropertyInBMS_STL_INFODF("credit_min_amt", "mer_no", item.getMerNo).getOrElse("-1")
              val creditMaxAmt = bmsStlInfoDF.queryPropertyInBMS_STL_INFODF("creditMaxAmt", "mer_no", item.getMerNo).getOrElse("-1")
              bmsStlInfoDF.queryPropertyInBMS_STL_INFODF("credit_calc_type", "mer_no", item.getMerNo) match {
                case Some(calType) => {
                  if (calType == "10"){
                    val creditCalcRate = bmsStlInfoDF.queryPropertyInBMS_STL_INFODF("credit_calc_rate", "mer_no", item.getMerNo).getOrElse("-1")
                    if (creditCalcRate.toDouble * item.getTxnAmt < creditMinAmt.toDouble) item.setExchange(creditMinAmt.toDouble)
                    else if (creditCalcRate.toDouble * item.getTxnAmt > creditMaxAmt.toDouble) item.setExchange(creditMaxAmt.toDouble)
                    else item.setExchange(creditCalcRate.toDouble * item.getTxnAmt)
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
              //            sumMapAccum += Map(item.getMerId.toString -> (item.getTxnAmt - item.getExchange))
              item
          }.toList
          newPartition.iterator
        }
      }
    }
    //保存汇总信息到HBase
//    val columnName = "sum"
//    for ((k, v) <- sumMapAccum.value){
//      hbaseUtils.writeTable(summaryName, k, columnName, v.toString)
//    }
    //sumMapAccum.toString()
    result.saveAsObjectFiles("ums_poc", ".obj")

    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop(true, true)

  }

  //def extendProperty(propertyValueMatched:String, propertyNameMatched:String, extendProperties:List[String], targetCacheRDD:IgniteRDD[])
}

