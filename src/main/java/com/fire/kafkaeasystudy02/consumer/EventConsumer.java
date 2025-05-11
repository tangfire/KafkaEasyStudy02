package com.fire.kafkaeasystudy02.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class EventConsumer {
    // 采用监听的方式接收事件(消息、数据)
    @KafkaListener(topics = {"helloTopic"},groupId = "helloGroup")
    public void onEvent(@Payload String event) {
        System.out.println("读取的事件："+event);
    }

}
