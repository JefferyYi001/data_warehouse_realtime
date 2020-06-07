package com.atguigu.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @time 2020/5/29 - 11:33
 * @Version 1.0
 * @Author Jeffery Yi
 */
case class StartupLog(mid: String,
                      uid: String,
                      appId: String,
                      area: String,
                      os: String,
                      channel: String,
                      logType: String,
                      version: String,
                      ts: Long,
                      var logDate: String = null, // 2020-05-29
                      var logHour: String = null) {
  private val date = new Date(ts)
  logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
  logHour = new SimpleDateFormat("HH").format(date)
}
