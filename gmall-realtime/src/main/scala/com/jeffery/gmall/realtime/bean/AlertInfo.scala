package com.jeffery.gmall.realtime.bean

import java.util

/**
 * @time 2020/6/1 - 20:05
 * @Version 1.0
 * @Author Jeffery Yi
 */
case class AlertInfo(
                      mid: String,
                      uids: util.HashSet[String],
                      itemIds: util.HashSet[String],
                      events: util.ArrayList[String],
                      ts: Long)
