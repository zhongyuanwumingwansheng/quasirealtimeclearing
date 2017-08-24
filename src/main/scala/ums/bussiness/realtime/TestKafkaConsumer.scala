package ums.bussiness.realtime

import com.typesafe.config.{Config, ConfigFactory}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object TestKafkaConsumer {
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
    val ulinkNormalTopicMap = scala.collection.immutable.Map("ULinkNormal" -> 1)
    val topicsSet: Set[String] = kafkaTopics.split("ULinkNormal").toSet
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> kafkaGroup,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "auto.offset.reset" -> "smallest" //自动将偏移重置为最早的偏移
    )
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamContext, kafkaParams, topicsSet).flatMap(line => Some(line._2.toString()))
    lines.print
    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop(true, true)
  }
}
