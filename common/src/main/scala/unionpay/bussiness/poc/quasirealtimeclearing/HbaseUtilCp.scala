package unionpay.bussiness.poc.quasirealtimeclearing

import java.util
import java.util.Date

import org.apache.hadoop.hbase.client._
//import it.unimi.dsi.fastutil.objects.{Object2ObjectOpenHashMap, ObjectArrayList, ObjectArraySet}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HTableDescriptor, HColumnDescriptor}
import org.apache.hadoop.hbase.util.Bytes


/**
  *
  * Created by supertool on 2016/6/30.
  */
class HbaseUtilCp(conf: HBaseConfiguration, connection: Connection, admin: Admin, DEFAULT_COLUMN_FAMILIES: String) extends Serializable {

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
  /*
  def delRowKey(tableName: String, rowKey: List[String]): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    try {
      val deleteList = new util.ArrayList[Delete]()
      for (i <- 0 until (rowKey.size)) {
        val delete = new Delete(Bytes.toBytes(rowKey(i)))
        deleteList.add(delete)
      }
      tableInterface.delete(deleteList)
      tableInterface.flushCommits()
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      if (tableInterface != null)
        tableInterface.close()
    }
  }
  */

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


object HbaseUtilCp extends Serializable {
  var hbaseU: HbaseUtilCp = null
  private var conf: HBaseConfiguration = null
  private var connection: Connection = null
  var admin: Admin = null

  def apply(zkHosts: String): HbaseUtilCp = {
    if (connection == null || hbaseU == null) {
      conf = new HBaseConfiguration()
      conf.set("hbase.zookeeper.quorum", zkHosts)
      connection = ConnectionFactory.createConnection(conf)
      println(connection)
      admin = connection.getAdmin
      println(admin)
      hbaseU = new HbaseUtilCp(conf, connection, admin, "data")
    }
    hbaseU
  }

  def main(args: Array[String]) {
    val hbaseUtil = HbaseUtilCp("182.180.125.183:2181,182.180.125.184:2181,182.180.125.185:2181")
    while (true) {
      val start_time = new Date().getTime
      //val result_01 = hbaseUtil.selectLatestColumnsByPrefix(TodayHistory.getTableName(TodayHistory.TBL_OJNL_DERIVED_RSLT), "A_0019105829308_2")
      //      val result_02 = hbaseUtil.selectColumnsByPrefix(TodayHistory.getTableName(TodayHistory.TBL_OJNL_DERIVED_RSLT), "A_0019243452387_2")
      //      result_01.foreach(println)
      //      result_02.foreach(println)
      val end_time = new Date().getTime
      val interval = end_time - start_time
      println(interval)
    }
  }
}

