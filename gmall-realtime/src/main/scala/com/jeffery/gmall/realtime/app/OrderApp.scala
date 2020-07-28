package com.jeffery.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.jeffery.gmall.common.Constant
import com.jeffery.gmall.realtime.bean.OrderInfo
import com.jeffery.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @time 2020/6/1 - 10:53
 * @Version 1.0
 * @Author Jeffery Yi
 */
object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("OrderApp")
    val ssc = new StreamingContext(conf, Seconds(3))

    // 1. 从 kafka 消费数据
    val sourceStream: DStream[String] = MyKafkaUtil
      .getKafkaStream(ssc, Constant.ORDER_INFO_TOPIC)
    // 2. 封装数据
    val orderInfoStream: DStream[OrderInfo] = sourceStream
      .map(log => JSON.parseObject(log, classOf[OrderInfo]))
    // 3. 将数据写入 hbase（通过 Phoenix）
    import org.apache.phoenix.spark._
    orderInfoStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT",
          "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID",
          "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS",
          "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID",
          "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    // 4. 用于测试
    orderInfoStream.print
    ssc.start()
    ssc.awaitTermination()
  }
}

