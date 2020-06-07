package com.jeffery.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.jeffery.gmallpublisher.bean.Option;
import com.jeffery.gmallpublisher.bean.SaleInfo;
import com.jeffery.gmallpublisher.bean.Stat;
import com.jeffery.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @time 2020/5/29 - 20:28
 * @Version 1.0
 * @Author Jeffery Yi
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherService service;

    // http://localhost:8070/realtime-total?date=2020-05-31
    @GetMapping("/realtime-total")
    public String realTotal(@RequestParam("date") String date){

        ArrayList<Map<String, String>> result = new ArrayList<>();

        HashMap<String, String> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", service.getDau(date).toString());
        result.add(map1);

        HashMap<String, String> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", "233");
        result.add(map2);

        HashMap<String, String> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        map3.put("value", service.getTotalAmount(date).toString());
        result.add(map3);

        return JSON.toJSONString(result);
    }

    // http://localhost:8070/realtime-hour?id=dau&date=2020-05-31
    // http://localhost:8070/realtime-hour?id=order_amount&date=2020-02-14
    @GetMapping("/realtime-hour")
    // 变量名相同时 @RequestParam("date") 可以省略
    public String realTimeHour(String id, String date){
        if ("dau".equals(id)){
            Map<String, Long> today = service.getHourDau(date);
            Map<String, Long> yesterday = service.getHourDau(getYesterday(date));

            Map<String, Map<String, Long>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);
            return JSON.toJSONString(result);
        } else if ("order_amount".equals(id)){
            Map<String, Double> today = service.getHourAmount(date);
            Map<String, Double> yesterday = service.getHourAmount(getYesterday(date));

            Map<String, Map<String, Double>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);
            return JSON.toJSONString(result);
        }

        return "ok";
    }

    private String getYesterday(String date) {
        return LocalDate.parse(date).minusDays(1).toString();
    }

    //http://localhost:8070/sale_detail?date=2020-06-06&&startpage=1&&size=5&&keyword=手机小米
    @GetMapping("/sale_detail")
    public String saleDetail(String date, int startpage, int size, String keyword) throws IOException {
        // 1. 获取 server 返回的数据
        Map<String, Object> genderResult = service
                .getSaleDetailAndAgg(date, keyword, startpage, size, "user_gender", 2);
        Map<String, Object> ageResult = service
                .getSaleDetailAndAgg(date, keyword, startpage, size, "user_age", 100);
        // 2. 创建 SaleInfo 对象用于封装数据
        SaleInfo saleInfo = new SaleInfo();
        // 向结果封装数据
        //  2.1 封装总数
        Integer total = (Integer) genderResult.get("total");
        saleInfo.setTotal(total);
        //  2.2 封装明细
        List<HashMap> detail = (List<HashMap>) genderResult.get("detail");
        saleInfo.setDetail(detail);
        //  2.3 封装饼图
        //      2.3.1 封装性别饼图
        Stat genderStat = new Stat();
        genderStat.setTitle("用户性别分布");
        Map<String, Long> aggGender = (Map<String, Long>) genderResult.get("agg");
        for (String key : aggGender.keySet()) {
            Option opt = new Option();
            opt.setName(key.equals("M") ? "男": "女");
            opt.setValue(aggGender.get(key));
            genderStat.addOption(opt);
        }
        saleInfo.addStat(genderStat);
        //      2.3.2 封装年龄饼图
        Stat ageStat = new Stat();
        ageStat.addOption(new Option("20以下", 0L));
        ageStat.addOption(new Option("20以上30以下", 0L));
        ageStat.addOption(new Option("30以上", 0L));
        Map<String, Long> aggAge = (Map<String, Long>) ageResult.get("agg");

        for (Map.Entry<String, Long> entry : aggAge.entrySet()) {
            int age = Integer.parseInt(entry.getKey());
            Long value = entry.getValue();
            if (age < 20){
                Option opt = ageStat.getOptions().get(0);
                Long newValue = opt.getValue() + value;
                opt.setValue(newValue);
            } else if (age < 30){
                Option opt = ageStat.getOptions().get(1);
                Long newValue = opt.getValue() + value;
                opt.setValue(newValue);
            } else {
                Option opt = ageStat.getOptions().get(2);
                Long newValue = opt.getValue() + value;
                opt.setValue(newValue);
            }
        }
        saleInfo.addStat(ageStat);

        return JSON.toJSONString(saleInfo);
    }
}
