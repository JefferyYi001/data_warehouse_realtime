package com.jeffery.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.jeffery.gmall.common.Constant
import com.jeffery.gmall.realtime.bean.StartupLog
import com.jeffery.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @time 2020/5/28 - 16:21
 * @Version 1.0
 * @Author Jeffery Yi
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))

    val sourceStream = MyKafkaUtil.getKafkaStream(ssc, Constant.STARTUP_TOPIC)

    val startupLogStream: DStream[StartupLog] = sourceStream
      .map(log => JSON.parseObject(log, classOf[StartupLog]))

    val filteredStartupStream = startupLogStream
      .transform(rdd => {
        val client = RedisUtil.getClient
        val mids = client.smembers(Constant.STARTUP_TOPIC + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
        client.close()

        val midsBC = ssc.sparkContext.broadcast(mids)

        rdd
          .filter(startupLog => !midsBC.value.contains(startupLog.mid))
          .map(startupLog => (startupLog.mid, startupLog))
          .groupByKey()
          .map {
            case (mid, it) => it.toList.minBy(_.ts)
          }
      })

    filteredStartupStream.foreachRDD(rdd => {
      rdd.foreachPartition(startupLogIt => {
        val client = RedisUtil.getClient
        startupLogIt.foreach(startupLog => {
          client.sadd(Constant.STARTUP_TOPIC + ":" + startupLog.logDate, startupLog.mid)
        })
        client.close()
      })

      import org.apache.phoenix.spark._
      rdd.saveToPhoenix(
        "GMALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    filteredStartupStream.print

    ssc.start()
    ssc.awaitTermination()
  }
}
