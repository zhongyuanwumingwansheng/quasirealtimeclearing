package com.ums

import java.util.Properties
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.api.java.StorageLevels
import com.typesafe.config._
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.kie.api.KieServices
import kafka.producer.ProducerConfig
import kafka.producer.KeyedMessage
import kafka.javaapi.producer.Producer
import org.kie.api.builder.KieBuilder
import org.kie.api.builder.KieScanner
import org.kie.api.builder.ReleaseId
import org.kie.api.runtime.KieContainer
import org.kie.api.runtime.KieSession
/**
  * Created by zhaikaixuan on 27/07/2017.
  */
object ApplyULinkIncreRuleToSparkStream extends Logging{
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
    val sqlContext = new SQLContext(sc)
    //val topicMapUlinkIncremental = {}
    //val topicMapULinkTraditional = {}
    val topicMap = kafkaTopics.split(",").map((_, kafkaThread)).toMap
    val kafkaStreams = (1 to kafkaReceiverNum).map { _ =>
      KafkaUtils.createStream(streamContext, kafkaZkHost, kafkaGroup, topicMap, StorageLevels.MEMORY_AND_DISK_SER)
    }
    val lines = streamContext.union(kafkaStreams)

    val sys_group_item_info_df = sqlContext.read.parquet("table path").select("Mchnt_Id_Pay", "category")

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
    val sendToKafkaInc:SendMessage = new SendToKafka(kafkaProducer, "ulink_incremental")
    //val sendToKafkaTra:SendMessage = new SendToKafka(kafkaProducer, "ulink_traditional")

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
    val queryServiceImp:QueryRelatedProperty = new QueryRelatedPropertyInDF(sys_group_item_info_df)
    kieSession.setGlobal("queryService", queryServiceImp)
    val kSession = sc.broadcast(kieSession)
    case class Item(aspect:String)
    lines.foreachRDD{
      DiscretizedRdd =>
        DiscretizedRdd.foreachPartition{
          partition => partition.map {
            item =>
              val itemAfterParsing = Item(item._1) //parsing
              //var extendedItem = extendProperty(itemAfterParsing)
              // val cacheRDD = igniteContext.fromCache("sys_group_item_info_rdd")
              //val result = cacheRDD.sql("select ")
              //itemAfterParsing
              //val message = new KeyedMessage[String, String]("topic", "message")
              //kafkaProducer.send(message) //moved to rules
              kSession.value.insert(itemAfterParsing)
          }
            kSession.value.fireAllRules()
      }
    }
    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop(true, true)

  }

  //def extendProperty(propertyValueMatched:String, propertyNameMatched:String, extendProperties:List[String], targetCacheRDD:IgniteRDD[])
}
