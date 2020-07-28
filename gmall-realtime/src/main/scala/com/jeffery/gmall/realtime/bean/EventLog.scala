package com.jeffery.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @time 2020/6/1 - 19:00
 * @Version 1.0
 * @Author Jeffery Yi
 */
case class EventLog(mid: String,
                    uid: String,
                    appId: String,
                    area: String,
                    os: String,
                    logType: String,
                    eventId: String,
                    pageId: String,
                    nextPageId: String,
                    itemId: String,
                    ts: Long,
                    var logDate: String = null,
                    var logHour: String = null) {
  private val date = new Date(ts)
  logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
  logHour = new SimpleDateFormat("HH").format(date)
}
