package com.jeffery.gmallpublisher.util

import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig

/**
 * @time 2020/6/7 - 9:27
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

  def getClient = factory.getObject

  def getDSL(date: String,
             keyword: String,
             startPage: Int,
             sizePerPage: Int,
             aggField: String,
             aggCount: Int) = {
    s"""
       |{
       |  "query": {
       |    "bool": {
       |      "filter": {
       |        "term": {
       |          "dt": "$date"
       |        }
       |      },
       |      "must": [
       |        {"match": {
       |          "sku_name": {
       |            "query": "$keyword",
       |            "operator": "and"
       |          }
       |        }}
       |      ]
       |    }
       |  },
       |  "aggs": {
       |    "group_by_$aggField": {
       |      "terms": {
       |        "field": "$aggField",
       |        "size": $aggCount
       |      }
       |    }
       |  },
       |  "from": ${(startPage - 1) * sizePerPage},
       |  "size": $sizePerPage
       |}
       |""".stripMargin
  }
}
