package com.atguigu.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @time 2020/5/29 - 23:41
 * @Version 1.0
 * @Author Jeffery Yi
 */
object RedisUtil {

  private val host = "hadoop103"
  private val port = 6379
  private val conf = new JedisPoolConfig
  conf.setMaxTotal(100)
  conf.setMaxIdle(40)
  conf.setMinIdle(10)
  conf.setBlockWhenExhausted(true)
  conf.setMaxWaitMillis(60 * 1000)
  conf.setTestOnBorrow(true)
  conf.setTestOnCreate(true)
  conf.setTestOnReturn(true)

  private val pool = new JedisPool(conf, host, port)

  def main(args: Array[String]): Unit = {
    val client = pool.getResource
    println(client.get("k1"))
  }

  def getClient = {
    //    pool.getResource
    val client = new Jedis(host, port, 60 * 1000)
    client.connect()
    client
  }

}
