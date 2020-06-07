package com.jeffery.gmallpublisher.service;

import com.jeffery.gmallpublisher.mapper.DauMapper;
import com.jeffery.gmallpublisher.mapper.OrderMapper;
import com.jeffery.gmallpublisher.util.ESUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @time 2020/5/29 - 21:10
 * @Version 1.0
 * @Author Jeffery Yi
 */
@Service
public class PublisherServiceImp implements PublisherService {
    @Autowired
    DauMapper dau;
    @Override
    public Long getDau(String date) {
        return dau.getDau(date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {

        List<Map<String, Object>> hourDauList = dau.getHourDau(date);

        Map<String, Long> result = new HashMap<>();

        for (Map<String, Object> map : hourDauList) {
            String loghour = (String)map.get("LOGHOUR");
            Long count = (Long)map.get("COUNT");
            result.put(loghour, count);
        }

        return result;
    }

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Double getTotalAmount(String date) {
        Double totalAmount = orderMapper.getTotalAmount(date);
        return totalAmount == null ? 0 : totalAmount;
    }

    @Override
    public Map<String, Double> getHourAmount(String date) {
        List<Map<String, Object>> hourAmountList = orderMapper.getHourAmount(date);
        Map<String, Double> result = new HashMap<>();
        for (Map<String, Object> map : hourAmountList) {
            String key = (String) map.get("CREATE_HOUR");
            double value = ((BigDecimal) map.get("SUM")).doubleValue();
            result.put(key, value);
        }
        return result;
    }

    @Override
    public Map<String, Object> getSaleDetailAndAgg(String date,
                                                   String keyword,
                                                   int startPage,
                                                   int sizePerPage,
                                                   String aggField,
                                                   int aggCount) throws IOException {
        // 1. 获取查询字符串
        String dsl = ESUtil.getDSL(date, keyword, startPage, sizePerPage, aggField, aggCount);
        // 2. 获取 ES 客户端
        JestClient client = ESUtil.getClient();
        Search search = new Search.Builder(dsl).build();
        // 3. 执行查询，返回所有结果
        SearchResult searchResult = client.execute(search);
        // 4. 从 SearchResult 中解析出我们想要的数据
        HashMap<String, Object> result = new HashMap<>();
        //  4.1 总数
        Integer total = searchResult.getTotal();
        result.put("total", total);
        //  4.2 详情
        ArrayList<HashMap> detail = new ArrayList<>();
        List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
        for (SearchResult.Hit<HashMap, Void> hit : hits) {
            HashMap source = hit.source;
            detail.add(source);
        }
        result.put("detail", detail);
        // 4.3 聚合结果
        HashMap<String, Long> agg = new HashMap<>();
        List<TermsAggregation.Entry> buckets = searchResult
                .getAggregations()
                .getTermsAggregation("group_by_" + aggField)
                .getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            String key = bucket.getKey();
            Long value = bucket.getCount();
            agg.put(key, value);
        }
        result.put("agg", agg);

        return result;
    }
}