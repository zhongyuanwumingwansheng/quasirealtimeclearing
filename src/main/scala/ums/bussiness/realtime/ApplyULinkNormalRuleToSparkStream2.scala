package ums.bussiness.realtime

import java.util.Properties
import java.lang.{Long => JLong}

import com.typesafe.config._
import org.apache.ignite.Ignition
import org.apache.ignite.cache.query.SqlQuery
import org.apache.ignite.configuration.IgniteConfiguration
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

import scala.collection.mutable.{ArrayBuffer, HashMap}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{Ignite, IgniteCache}
import ums.bussiness.realtime.common.IgniteUtil

/**
  * Created by zhaikaixuan on 27/07/2017.
  */
object ApplyULinkNormalRuleToSparkStream2 extends Logging {
  implicit val format = Serialization.formats(ShortTypeHints(List()))
  private val CONFIG = "example-ignite.xml"


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
    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val streamContext = new StreamingContext(conf, Milliseconds(processInterval))
    streamContext.sparkContext.setLogLevel(logLevel)
    val sc = streamContext.sparkContext
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    val topicMap = kafkaTopics.split(",").map((_, kafkaThread)).toMap
    val kafkaStreams = (1 to kafkaReceiverNum).map { _ =>
      KafkaUtils.createStream(streamContext, kafkaZkHost, kafkaGroup, topicMap, StorageLevels.MEMORY_AND_DISK_SER)
    }
    val lines = streamContext.union(kafkaStreams).map(_._2)

    lines.print

    //解析数据
    val records = lines.map(line => {
      val record = new HashMap[String, String]
      val formatLine = line.replaceAll("\":null\"", ":\"\"")
      val ulinkNormal = new UlinkNormal()
      try {
        val JObject(message) = parse(formatLine).asInstanceOf[JObject]
        message.map(keyValue =>
          //            record.put(keyValue._1,keyValue._2.extract[String])
          keyValue._1 match {
            case "MSG_TYPE" => ulinkNormal.setMsgType(keyValue._2.extract[String])
            case "PROC_CODE" => ulinkNormal.setProcCode(keyValue._2.extract[String])
            case "SER_CONCODE" => ulinkNormal.setSerConcode(keyValue._2.extract[String])
            case "TXN_AMT" => ulinkNormal.setTxnAmt(keyValue._2.extract[String].toDouble)
            case "TID" => ulinkNormal.settId(keyValue._2.extract[String])
            case "MID" => ulinkNormal.setmId(keyValue._2.extract[String])
            case "TRAN_STATUS" => ulinkNormal.setTranStat(keyValue._2.extract[String])
            case "RESP_CODE" => ulinkNormal.setRespCode(keyValue._2.extract[String])
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
      val cacheName = "records"
      val filterRdocords = new ArrayBuffer[UlinkNormal]
      iter =>
        val ignite = IgniteUtil("")
        cache$(cacheName).get.destroy()
        createCache$(cacheName, indexedTypes = Seq(classOf[String], classOf[UlinkNormal]))
        iter.foreach { record =>
          cache$(cacheName).get.put(record.getmId(),record)
        }

        val sql = "(txnAmt > 500 and respCode != 01) or (respCode != 00 and respCode != 10)"
        val result = cache$[String, UlinkNormal](cacheName).get.sql(sql).getAll
        val result_iterator = result.iterator()
        while (result_iterator.hasNext) {
          val record = result_iterator.next()
          filterRdocords += record.getValue
        }
        filterRdocords.iterator
    }

    //交易码转换

    //清分规则定位与计算
    //清分规则 ID 获取
    //按终端入账
    //根据流水信息映射商户号
    filterRecoders.foreachRDD{rdd =>

      rdd.foreach{record =>
        println(record.getmId())
      }
    }


    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop(true, true)

  }

  //def extendProperty(propertyValueMatched:String, propertyNameMatched:String, extendProperties:List[String], targetCacheRDD:IgniteRDD[])
}

