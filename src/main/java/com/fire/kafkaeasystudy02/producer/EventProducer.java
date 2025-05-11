package com.fire.kafkaeasystudy02.producer;


import jakarta.annotation.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class EventProducer {

    // 加入了spring-kafka依赖 + .yml配置信息，springboot自动配置好了kafka，自动装配好了KafkaTemplate这个Bean
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;



    public void sendEvent() {
        kafkaTemplate.send("helloTopic","hello kafka");
    }





}
