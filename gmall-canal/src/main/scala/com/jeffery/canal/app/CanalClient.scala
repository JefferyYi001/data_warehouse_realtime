package com.jeffery.canal.app

import java.net.InetSocketAddress
import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.client.CanalConnectors
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange}
import com.jeffery.canal.util.MyKafkUtil
import com.jeffery.gmall.common.Constant

import scala.collection.JavaConversions._
import scala.util.Random

/**
 * @time 2020/5/31 - 11:34
 * @Version 1.0
 * @Author Jeffery Yi
 */
object CanalClient {

  def parseData(rowDatasList: util.List[CanalEntry.RowData], tableName: String, eventType: EventType) = {
    if (tableName == "order_info" && eventType == EventType.INSERT && rowDatasList != null && rowDatasList.size() > 0) {
      sendToKafa(Constant.ORDER_INFO_TOPIC, rowDatasList)
    } else if (tableName == "order_detail" && eventType == EventType.INSERT && rowDatasList != null && rowDatasList.size() > 0) {
      sendToKafa(Constant.ORDER_DETAIL_TOPIC, rowDatasList)
    }
  }

  def sendToKafa(topic: String, rowDatasList: util.List[CanalEntry.RowData]): Unit = {
    for (rowData <- rowDatasList) {
      val result = new JSONObject()
      val columnsList = rowData.getAfterColumnsList
      for (column <- columnsList) {
        val key = column.getName
        val value = column.getValue
        result.put(key, value)
      }
      // 发送前打印（用于测试）
      println(result)
      // 直接发送到 kafka
      // MyKafkUtil.send(topic, result.toJSONString)
      // 模拟网络延迟的场景
      newThread{
        MyKafkUtil.send(topic, result.toJSONString)
      }

    }
  }

  def newThread(code: => Unit) = {
    new Thread() {
      override def run(): Unit = {
        Thread.sleep(new Random().nextInt(1000 * 10))
        code
      }
    }.start()
  }


  def main(args: Array[String]): Unit = {
    val address = new InetSocketAddress("hadoop103", 11111)
    val connector = CanalConnectors
      .newSingleConnector(address, Constant.CANAL_DESTINATION, Constant.CANAL_USERNAME, Constant.CANAL_PASSWORD)
    connector.connect()
    connector.subscribe("gmall1128.*")

    while (true) {
      val msg = connector.get(100)
      val entries = msg.getEntries
      if (entries != null && entries.size() > 0) {
        import scala.collection.JavaConversions._
        for (entry <- entries) {
          if (entry != null && entry.hasEntryType && entry.getEntryType == EntryType.ROWDATA) {
            val storeValue = entry.getStoreValue
            val rowChange = RowChange.parseFrom(storeValue)
            val rowDatasList = rowChange.getRowDatasList
            parseData(rowDatasList, entry.getHeader.getTableName, rowChange.getEventType)
          }
        }
      } else {
        println("没有拉到数据，2S后重试")
        Thread.sleep(2000)
      }
    }
  }
}
