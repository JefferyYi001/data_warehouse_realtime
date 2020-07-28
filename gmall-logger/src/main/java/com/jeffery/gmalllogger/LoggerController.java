package com.jeffery.gmalllogger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jeffery.gmall.common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


/**
 * @time 2020/5/27 - 19:23
 * @Version 1.0
 * @Author Jeffery Yi
 */
@RestController
public class LoggerController {


    @PostMapping("/log")
    public String doLogger(@RequestParam("log") String log){
        log = addTS(log);
        saveToDisk(log);
        sendToKafka(log);

        return "ok";
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private void sendToKafka(String log) {
        if (log.contains("startup")){
            kafkaTemplate.send(Constant.STARTUP_TOPIC, log);
        }else {
            kafkaTemplate.send(Constant.EVENT_TOPIC, log);
        }
    }

    private Logger logger = LoggerFactory.getLogger(LoggerController.class);

    private void saveToDisk(String log) {
        logger.info(log);
    }

    private String addTS(String log) {
        JSONObject jsonObj = JSON.parseObject(log);
        jsonObj.put("ts", System.currentTimeMillis());
        return jsonObj.toJSONString();
    }
}
