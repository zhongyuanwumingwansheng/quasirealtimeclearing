package ums.bussiness.realtime.common

import com.typesafe.config.Config
import org.apache.ignite.cache.query.SqlQuery
import org.apache.ignite.configuration.{IgniteConfiguration, MemoryConfiguration}
import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.scalar.scalar._
import ums.bussiness.realtime.model.flow.UlinkNormal

class IgniteUtil(ignite:Ignite) extends Serializable{
  def createCache(cacheName: String): Unit = {
      ignite.createCache(cacheName)
  }

  def putCache[T](cacheName:String,key:String,data:T):Unit ={

    ignite.getOrCreateCache(cacheName).put(key,data)
  }

  def queryCache[T](cacheName:String,sql:String,data:T):Unit ={
    val result = ignite.cache(cacheName).sql("");
    result
  }
}

object IgniteUtil extends Serializable {
  var igniteConfiguration = new IgniteConfiguration
  var ignite: Ignite = null
  var igniteUtil: IgniteUtil = null

  def apply(): IgniteUtil = {
    //    igniteConfiguration = new IgniteConfiguration
    //    ignite = Ignition.start(igniteConfiguration)
    if (igniteUtil == null) {
//      igniteConfiguration.setClientMode(true)
      ignite = Ignition.start(igniteConfiguration)
      igniteUtil = new IgniteUtil(ignite)
    }
    igniteUtil
  }

  def apply(setting: Config): Ignite = {
    //    igniteConfiguration = new IgniteConfiguration
    //    ignite = Ignition.start(igniteConfiguration)
    if (ignite == null) {
//      igniteConfiguration.setClientMode(true)
//      igniteConfiguration.setMemoryConfiguration(memoryConfiguration)
      ignite = Ignition.start(igniteConfiguration)
    }
    ignite
  }
  def main(args: Array[String]): Unit = {
    //{"MSG_TYPE": "0200","PROC_CODE": "000000","SER_CONCODE": "00","TXN_AMT": "297.00","TID": "20222636","MID": "898510158120962","TRAN_STATUS": "1","RESP_CODE": "00"}

        val igniteUtil = IgniteUtil()
        System.out.println("ignite is begin")
        for (i <- 0 until 10) {
          println("record : " + i)
          igniteUtil.putCache("cacheName","key" + i,new UlinkNormal())
      }
        for (i <- 0 until 10) {
          val result = igniteUtil.queryCache("cacheName","key" + i,new UlinkNormal())
          System.out.println(result)
        }
    }
}
