package com.atguigu.gmall.realtime.util

import com.atguigu.gmall.common.util.PropertyUtil
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * @time 2020/5/28 - 16:28
 * @Version 1.0
 * @Author Jeffery Yi
 */
object MyKafkaUtil {
  def getKafkaStream(ssc: StreamingContext, STARTUP_TOPIC: String) = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      params,
      Set(STARTUP_TOPIC))
      .map(_._2)
  }

  val params = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertyUtil.getProperty("config.properties", "kafka.servers"),
    ConsumerConfig.GROUP_ID_CONFIG-> PropertyUtil.getProperty("config.properties", "kafkal.group.id")
  )



}
