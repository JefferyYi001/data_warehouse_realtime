package com.jeffery.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

/**
 * @time 2020/5/29 - 20:28
 * @Version 1.0
 * @Author Jeffery Yi
 */
public interface PublisherService {
    Long getDau(String date);

    Map<String, Long> getHourDau(String date);

    Double getTotalAmount(String date);

    Map<String, Double> getHourAmount(String date);

    Map<String, Object> getSaleDetailAndAgg(String date,
                                            String keyword,
                                            int startPage,
                                            int sizePerPage,
                                            String aggField,
                                            int aggCount) throws IOException;
}
