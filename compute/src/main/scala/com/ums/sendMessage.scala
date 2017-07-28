package com.ums
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage

/**
  * Created by zhaikaixuan on 28/07/2017.
  */
trait SendMessage{
  def sendMessage(message:String)
}

class SendToKafka(producer:Producer[String, String], topic:String) extends SendMessage{
  override def sendMessage(message: String) = {
    val keyMessage = new KeyedMessage[String, String](topic, message)
    producer.send(keyMessage)
  }
}
