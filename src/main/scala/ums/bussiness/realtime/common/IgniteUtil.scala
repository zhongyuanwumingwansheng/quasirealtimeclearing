package ums.bussiness.realtime.common

import com.typesafe.config.Config
import org.apache.ignite.cache.query.SqlQuery
import org.apache.ignite.configuration.{ConnectorConfiguration, IgniteConfiguration, MemoryConfiguration}
import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import ums.bussiness.realtime.model.flow.UlinkNormal

import scala.collection.JavaConverters._

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

//  def apply(): IgniteUtil = {
//    if (igniteUtil == null) {
//      igniteConfiguration.setClientMode(true)
//      ignite = Ignition.start(igniteConfiguration)
//      igniteUtil = new IgniteUtil(ignite)
//    }
//    igniteUtil
//  }

  def apply(setting: Config): Ignite = {
    //    igniteConfiguration = new IgniteConfiguration
    //    ignite = Ignition.start(igniteConfiguration)
    if (ignite == null) {
      val memoryConfiguration = new MemoryConfiguration
      igniteConfiguration.setMemoryConfiguration(memoryConfiguration)
      val spi: TcpDiscoverySpi = new TcpDiscoverySpi
      val ipFinder: TcpDiscoveryVmIpFinder = new TcpDiscoveryVmIpFinder
      print("**********************************" + setting.getString("tcpDiscoveryIpList"));
      ipFinder.setAddresses(setting.getString("tcpDiscoveryIpList").split(",").toSeq.asJavaCollection)
      spi.setIpFinder(ipFinder)
      igniteConfiguration.setDiscoverySpi(spi)
      igniteConfiguration.setClientMode(true)
//      val connectorConfiguration = new ConnectorConfiguration()
//      connectorConfiguration.setThreadPoolSize(1000)
//      connectorConfiguration.setSendQueueLimit(1000)
//      connectorConfiguration.setNoDelay(true)
//      connectorConfiguration.setSelectorCount(1000)
//      igniteConfiguration.setConnectorConfiguration(connectorConfiguration)
      ignite = Ignition.start(igniteConfiguration)
    }
    ignite
  }
  def main(args: Array[String]): Unit = {
    //
    //        val igniteUtil = IgniteUtil()
    //        System.out.println("ignite is begin")
    //        for (i <- 0 until 10) {
    //          println("record : " + i)
    //          igniteUtil.putCache("cacheName","key" + i,new UlinkNormal())
    //      }
    //        for (i <- 0 until 10) {
    //          val result = igniteUtil.queryCache("cacheName","key" + i,new UlinkNormal())
    //          System.out.println(result)
    //        }
    //    }
  }
}
