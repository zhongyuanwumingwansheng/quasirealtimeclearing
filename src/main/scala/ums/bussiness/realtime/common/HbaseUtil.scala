package ums.bussiness.realtime.common

import java.util
import java.util.Date

import org.apache.hadoop.hbase.client._
//import it.unimi.dsi.fastutil.objects.{Object2ObjectOpenHashMap, ObjectArrayList, ObjectArraySet}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HTableDescriptor, HColumnDescriptor}
import org.apache.hadoop.hbase.util.Bytes

import com.typesafe.config._


/**
  *
  * Created by supertool on 2016/6/30.
  */
class HbaseUtil(conf: HBaseConfiguration, connection: Connection, admin: Admin, DEFAULT_COLUMN_FAMILIES: String) extends Serializable {

  def createTable(tableName: String): Unit = {
    try {
      val tablename = TableName.valueOf(tableName)
      if (!admin.tableExists(tablename)) {
        val table = new HTableDescriptor(tablename)
        table.addFamily(new HColumnDescriptor(DEFAULT_COLUMN_FAMILIES))
        admin.createTable(table)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def createTable(tableName: String, columnFamily: String): Unit = {
    try {
      val tablename = TableName.valueOf(tableName)
      if (!admin.tableExists(tablename))  {
        val table = new HTableDescriptor(TableName.valueOf(tableName))
        table.addFamily(new HColumnDescriptor(columnFamily))
        admin.createTable(table)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def closeConnection() = {
    if (!connection.isClosed) {
      println("close connection")
      connection.close
    }
  }

  def delRowKey(tableName: String, rowKey: List[String]): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    try {
      val deleteList = new util.ArrayList[Delete]()
      for (i <- 0 until (rowKey.size)) {
        val delete = new Delete(Bytes.toBytes(rowKey(i)))
        deleteList.add(delete)
      }
      tableInterface.delete(deleteList)
      //tableInterface.flushCommits()
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      if (tableInterface != null)
        tableInterface.close()
    }
  }

  def readTable(tableName: String, rowKey: String, column: String): String = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    var finalResult = ""
    try {
      val get = new Get(Bytes.toBytes(rowKey))
      get.addColumn(Bytes.toBytes("data"), Bytes.toBytes(column))
      val result = tableInterface.get(get)
      for (i <- 0 until (result.size())) {
        val cells = result.listCells()
        if (cells != null && !cells.isEmpty) {
          for (j <- 0 until (cells.size())) {
            val cell = cells.get(j)
            finalResult = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          }
        }
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    finally {
      if (tableInterface != null)
        tableInterface.close()
    }
    finalResult
  }


  def isColumnExist(tableName: String, rowKey: String, columnName: String): Boolean = {
    var existed = false
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    //    tableInterface.setAutoFlush(false,false)
    try {
      val get = new Get(Bytes.toBytes(rowKey))
      get.addColumn(Bytes.toBytes("data"), Bytes.toBytes(columnName))
      val result = tableInterface.get(get)
      existed = !result.isEmpty
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    finally {
      if (tableInterface != null)
        tableInterface.close()
    }
    existed
  }

  def writeTable(tableName: String, rowKey: String, columnName: String, value: String): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    //    tableInterface.setAutoFlush(false)
    try {
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(DEFAULT_COLUMN_FAMILIES), Bytes.toBytes(columnName), Bytes.toBytes(value))
      tableInterface.put(put)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    finally {
      if (tableInterface != null)
        tableInterface.close()
    }
  }

  def writeTable(tableName: String, keyValues: Map[String, Map[String, String]]): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    try {
      //      tableInterface.setAutoFlush(false)
      val iterator = keyValues.iterator
      val puts = new util.ArrayList[Put]
      while (iterator.hasNext) {
        val keyValue = iterator.next
        val rowKey = keyValue._1
        val columnValues = keyValue._2
        val iteraotrColumn = columnValues.iterator
        val put = new Put(Bytes.toBytes(rowKey))
        while (iteraotrColumn.hasNext) {
          val columnValue = iteraotrColumn.next
          val columnName = columnValue._1
          var value = columnValue._2
          if (value == null) value = ""
          put.addColumn(Bytes.toBytes(DEFAULT_COLUMN_FAMILIES), Bytes.toBytes(columnName), Bytes.toBytes(value))
        }
        puts.add(put)
      }
      tableInterface.put(puts)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    finally {
      if (tableInterface != null)
        tableInterface.close()
    }
  }
}


object HbaseUtil extends Serializable {
  var hbaseU: HbaseUtil = null
  private var conf: HBaseConfiguration = null
  private var connection: Connection = null
  var admin: Admin = null

  def apply(setting: Config): HbaseUtil = {
    if (connection == null || hbaseU == null) {
      conf = new HBaseConfiguration()
      conf.set("hbase.zookeeper.quorum", setting.getString("zookeeperHosts"))
      connection = ConnectionFactory.createConnection(conf)
      println(connection)
      admin = connection.getAdmin
      println(admin)
      hbaseU = new HbaseUtil(conf, connection, admin, "data")
    }
    hbaseU
  }

  def main(args: Array[String]) {
    val setting:Config = ConfigFactory.load
    val hbaseUtil = HbaseUtil(setting)
  }
}

