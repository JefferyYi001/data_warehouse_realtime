package com.atguigu.gmall.common.util

import java.util.Properties

/**
 * Author atguigu
 * Date 2020/5/27 16:27
 */
object PropertyUtil {
    def getProperty(fileName: String, propertyName: String) = {
        val is = PropertyUtil.getClass.getClassLoader.getResourceAsStream(fileName)
        val properties = new Properties()
        properties.load(is)
        properties.getProperty(propertyName)
    }
}
