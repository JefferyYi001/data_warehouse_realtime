package com.jeffery.gmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.jeffery.gmall.common.Constant
import com.jeffery.gmall.realtime.bean.{AlertInfo, EventLog}
import com.jeffery.gmall.realtime.util.{ESUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
 * @time 2020/6/4 - 9:19
 * @Version 1.0
 * @Author Jeffery Yi
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc = new StreamingContext(conf, Seconds(3))

    val sourceStream = MyKafkaUtil
      .getKafkaStream(ssc, Constant.EVENT_TOPIC).window(Minutes(5), Seconds(6))

    val eventLogStream = sourceStream
      .map(log => JSON.parseObject(log, classOf[EventLog]))

    val groupedEventLog = eventLogStream
      .map(eventLog => (eventLog.mid, eventLog))
      .groupByKey

    val alertInfoStream = groupedEventLog.map {
      case (mid, eventLogIt) =>
        val uidSet = new util.HashSet[String]()
        val eventList = new util.ArrayList[String]()
        val itemSet = new util.HashSet[String]()
        var isClickItem = false
        import scala.util.control.Breaks._
        breakable {
          eventLogIt.foreach(event => {
            eventList.add(event.eventId)
            event.eventId match {
              case "coupon" =>
                uidSet.add(event.uid)
                itemSet.add(event.itemId)
              case "clickItem" =>
                isClickItem = true
                break
              case _ =>
            }
          })
        }

        (!isClickItem && uidSet.size() >= 3, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
    }

    alertInfoStream
      .filter(_._1)
      .map(_._2)
      .foreachRDD(rdd => {
        import ESUtil._
        rdd.saveToES("gmall_coupon_alert1128")
      })

    alertInfoStream.print(1000)

    ssc.start()
    ssc.awaitTermination()
  }
}
