package com.atguigu.canal.util

import java.util.Properties

import com.atguigu.gmall.common.util.PropertyUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @time 2020/5/31 - 15:37
 * @Version 1.0
 * @Author Jeffery Yi
 */
object MyKafkUtil {
  val props = new Properties()
  props.setProperty("bootstrap.servers", PropertyUtil.getProperty("kafka.properties", "bootstrap.servers"))
  props.setProperty("key.serializer", PropertyUtil.getProperty("kafka.properties", "key.serializer"))
  props.setProperty("value.serializer", PropertyUtil.getProperty("kafka.properties", "value.serializer"))

  val producer = new KafkaProducer[String, String](props)

  def send(topic: String, content: String) = {
    producer.send(new ProducerRecord[String, String](topic, content))
  }
}
