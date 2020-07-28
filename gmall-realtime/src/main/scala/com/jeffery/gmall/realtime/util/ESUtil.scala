package com.jeffery.gmall.realtime.util

import com.jeffery.gmall.realtime.bean.AlertInfo
import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}
import org.apache.spark.rdd.RDD

/**
 * @time 2020/6/4 - 11:02
 * @Version 1.0
 * @Author Jeffery Yi
 */
object ESUtil {
  private val factory = new JestClientFactory
  private val esUrl = "http://hadoop103:9200"
  private val config = new HttpClientConfig.Builder(esUrl)
    .maxTotalConnection(100)
    .connTimeout(10000)
    .readTimeout(10000)
    .multiThreaded(true)
    .build()
  factory.setHttpClientConfig(config)

  def main(args: Array[String]): Unit = {
    /*    val source1 = User("aaa", 11)
        insertSingle("user1128", source1, "aaa")*/
    val it = Iterator(("bbb", User("bbb", 21)), ("ccc", User("ccc", 22)))
    insertBulk("user1128", sources = it)
  }

  def insertSingle(index: String, source: Object, id: String = null): Unit = {
    val client = factory.getObject
    val action = new Index.Builder(source)
      .index(index)
      .`type`("_doc")
      .id(id)
      .build()
    client.execute(action)
    client.shutdownClient()
  }

  def insertBulk(index: String, sources: Iterator[Object]): Unit = {
    val client = factory.getObject
    val bulk = new Bulk.Builder()
      .defaultIndex(index)
      .defaultType("_doc")
    sources.foreach {
      case (id: String, data) =>
        val action = new Index.Builder(data)
          .id(id)
          .build()
        bulk.addAction(action)
      case data =>
        val action = new Index.Builder(data)
          .build()
        bulk.addAction(action)
    }
    client.execute(bulk.build())
    client.shutdownClient()
  }

  implicit class RichES(rdd: RDD[AlertInfo]){
    def saveToES(index: String): Unit ={
      rdd.foreachPartition(it => {
        val source = it.map(info => {
          (info.mid + "_" + info.ts / 60 / 1000, info)
        })
        ESUtil.insertBulk("gmall_coupon_alert", source)
      })
    }
  }
}

case class User(name: String, age: Int)