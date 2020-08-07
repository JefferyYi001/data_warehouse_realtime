package com.jeffery.gmall.realtime.app

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.jeffery.gmall.common.Constant
import com.jeffery.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.jeffery.gmall.realtime.util.{ESUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis


/**
 * @time 2020/6/5 - 18:40
 * @Version 1.0
 * @Author Jeffery Yi
 */
object SaleDetailApp {

  private val url = "jdbc:mysql://hadoop103:3306/gmall1128"
  private val user = "root"
  private val pwd = "root"
  val prep = new Properties()
  prep.setProperty("user", user)
  prep.setProperty("password", pwd)

  def getOrderInfAndDetailStreams(ssc: StreamingContext): (DStream[OrderInfo], DStream[OrderDetail]) = {
    val orderInfoStream = MyKafkaUtil
      .getKafkaStream(ssc, Constant.ORDER_INFO_TOPIC)
      .map(line => JSON.parseObject(line, classOf[OrderInfo]))

    val orderDetailStream = MyKafkaUtil
      .getKafkaStream(ssc, Constant.ORDER_DETAIL_TOPIC)
      .map(line => JSON.parseObject(line, classOf[OrderDetail]))

    (orderInfoStream, orderDetailStream)
  }

  def saveToRedis(key: String, value: AnyRef, client: Jedis, timeout: Int) = {
    val content: String = Serialization.write(value)(DefaultFormats)
    client.setex(key, timeout, content)
  }

  def cacheOrderInfo(orderInfo: OrderInfo, client: Jedis) = {
    // 格式: "order_info:订单id"
    saveToRedis("order_info:" + orderInfo.id, orderInfo, client, 30 * 60)
  }

  def cacheOrderDetail(orderDetail: OrderDetail, client: Jedis) = {
    // 格式: "order_detail:订单id:详情id"
    saveToRedis("order_detail:" + orderDetail.order_id + ":" + orderDetail.id, orderDetail, client, 30 * 60)
  }

  def fullJoin(orderInfoStream: DStream[OrderInfo], orderDetailStream: DStream[OrderDetail]) = {
    val orderIdAndOrderInfo = orderInfoStream
      .map(info => (info.id, info))
    val orderIdAndOrderDetail = orderDetailStream
      .map(info => (info.order_id, info))

    orderIdAndOrderInfo
      .fullOuterJoin(orderIdAndOrderDetail)
      .mapPartitions((it: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) => {
        val client = RedisUtil.getClient
        val result = it.flatMap {
          case (orderId, (Some(orderInfo), Some(orderDetail))) =>
            //            println("(Some(orderInfo), Some(orderDetail))")
            // 1. 将 order_info 信息写入 Redis 缓存
            cacheOrderInfo(orderInfo, client)
            // 2. 进行 join，即将信息包装为样例类
            val saleDetail = SaleDetail()
              .mergeOrderInfo(orderInfo)
              .mergeOrderDetail(orderDetail)
            // 3. 再去 Redis 缓存中找数据进行 join
            // （1）先通过正则匹配获取相应的 key
            // （1）再遍历 key 集合获取相应的 value
            import scala.collection.JavaConversions._
            val keys = client.keys("order_detail:" + orderId + ":*").toList
            val saleDetails = keys.map(key => {
              val orderDetail = JSON
                .parseObject(client.get(key), classOf[OrderDetail])
              // 重点：删除对应的 key，否则会造成数据重复
              client.del(key)
              SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
            })
            saleDetail :: saleDetails
          case (orderId, (Some(orderInfo), None)) =>
            //            println("(Some(orderInfo), None)")
            // 1. 将 order_info 信息写入 Redis 缓存
            cacheOrderInfo(orderInfo, client)
            // 2. 再去 Redis 缓存中找数据进行 join
            // （1）先通过正则匹配获取相应的 key
            // （1）再遍历 key 集合获取相应的 value
            import scala.collection.JavaConversions._
            val keys = client.keys("order_detail:" + orderId + ":*").toList
            val saleDetails = keys.map(key => {
              val orderDetail = JSON
                .parseObject(client.get(key), classOf[OrderDetail])
              // 重点：删除对应的 key，否则会造成数据重复
              client.del(key)
              SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
            })
            saleDetails
          case (orderId, (None, Some(orderDetail))) =>
            //            println("(None, Some(orderDetail))")
            // 1. 去 order_info 的 Redis 缓存中找
            val orderInfoJson = client.get("order_info:" + orderId)
            if (orderInfoJson == null) {
              // 2. 如果不存在, 则写入 Redis 的 order_detail 缓存
              cacheOrderDetail(orderDetail, client)
              Nil
            } else {
              // 2. 如果存在，则 join
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
            }

        }
        // 关闭 Redis 客户端
        client.close()

        result
      })
  }

  def readUserInfo(spark: SparkSession, userIds: Array[String]) = {
    import scala.collection.JavaConversions._
    val client = RedisUtil.getClient
    // 1. 从 Redis 中读取用户信息并过滤出所需信息
    val userIdAndInfoList: List[(String, String)] = client
      .hgetAll("user_info")
      .toList
      .filter(userId => userIds.contains(userId))
    client.close
    // 2. 比较长度确定是否包含全部所需信息
    if (userIdAndInfoList.size == userIds.size) {
      // 2.1 长度相等代表包含全部所需信息，解析成样例类实例返回即可
      val userInfo = userIdAndInfoList.map {
        case (userId, jsonString) => (userId, JSON.parseObject(jsonString, classOf[UserInfo]))
      }
      spark.sparkContext.parallelize(userInfo)
    } else {
      // 2.2.1 长度不相等代表没有包含所有所需信息，需要从 MySQL 中读取全部 user_info 数据
      // 优化：每次仅从 MySQL 中读取所需数据
      val userIdsRdd = spark.sparkContext.parallelize(userIds)
      import spark.implicits._
      val userInfoRDD: RDD[(String, UserInfo)] = spark
        .read
        .jdbc(url, "user_info", prep)
        .join(userIdsRdd.toDF("uid"))
        .where($"id" === $"uid")
        .as[UserInfo]
        .map(info => (info.id, info))
        .rdd
      // 2.2.2 将读取之后的信息写入到 Redis
      userInfoRDD.foreachPartition((it: Iterator[(String, UserInfo)]) => {
        val client = RedisUtil.getClient
        it.foreach {
          case (userId, userInfo) =>
            // 数据类型为 key - hash
            client.hset("user_info", userId, Serialization.write(userInfo)(DefaultFormats))
        }
        client.close()
      })
      userInfoRDD
    }
  }

  def joinUser(saleDetailStream: DStream[SaleDetail], sc: SparkContext) = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()

    saleDetailStream.transform(rdd => {
      // 对 rdd 没有做缓存, collect 的时候, 就做了join, join的过程中, 会把redis的数据删除一部分.
      // 当真正 join 的时候, 如果没有缓存, 则会重新计算rdd, 但是这个时候出现问题: order_detail的信息已经没有了
      rdd.cache()
      // 1. 先将 saleDetailRDD 调整为 k-v 形式并缓存之
      val saleDetailRDD: RDD[(String, SaleDetail)] = rdd.map(detail => (detail.user_id, detail))
      // 2. 读取 user 数据 (附读缓存)
      val userInfoRDD = readUserInfo(spark, rdd.map(_.user_id).distinct().collect)
      // 3. 与 user 进行内连接
      saleDetailRDD
        .join(userInfoRDD)
        .map {
          case (_, (saleDetail, userInfo)) =>
            saleDetail.mergeUserInfo(userInfo)
        }
    })
  }

  def saveToES(resultStream: DStream[SaleDetail]) = {
    resultStream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        ESUtil.insertBulk("gmall_sale_detail", it)
      })
    })
  }


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 1. 获取两个流
    val (orderInfoStream, orderDetailStream) = getOrderInfAndDetailStreams(ssc)
    // 2. 进行双流 join, 实现一个宽表的效果
    val saleDetailStream: DStream[SaleDetail] = fullJoin(orderInfoStream, orderDetailStream)
    // 3. 上一步的结果再与 userInfo 进行 join, 通过反查 MySQL 实现
    val resultStream = joinUser(saleDetailStream, ssc.sparkContext)
    // 4. 将宽表数据写入 ES
    saveToES(resultStream)
    // 打印测试
    //    saleDetailStream.print
    resultStream.print
    ssc.start()
    ssc.awaitTermination()
  }
}
