package com.jeffery.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @time 2020/5/29 - 20:28
 * @Version 1.0
 * @Author Jeffery Yi
 */
public interface DauMapper {
    Long getDau(String date);
    List<Map<String, Object>> getHourDau(String date);
}
