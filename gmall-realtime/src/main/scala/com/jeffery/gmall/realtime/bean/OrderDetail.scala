package com.jeffery.gmall.realtime.bean

/**
 * @time 2020/6/5 - 12:30
 * @Version 1.0
 * @Author Jeffery Yi
 */
case class OrderDetail(id: String,
                       order_id: String,
                       sku_name: String,
                       sku_id: String,
                       order_price: String,
                       img_url: String,
                       sku_num: String)
