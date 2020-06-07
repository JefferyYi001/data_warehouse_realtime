package com.jeffery.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @time 2020/6/1 - 14:22
 * @Version 1.0
 * @Author Jeffery Yi
 */
public interface OrderMapper {
    Double getTotalAmount(String date);

    List<Map<String, Object>> getHourAmount(String date);
}
