package com.fire.kafkaeasystudy02.producer;


import com.fire.kafkaeasystudy02.model.User;
import com.fire.kafkaeasystudy02.util.JSONUtils;
import jakarta.annotation.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class EventProducer {

    // 加入了spring-kafka依赖 + .yml配置信息，springboot自动配置好了kafka，自动装配好了KafkaTemplate这个Bean
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;



    public void sendEvent() {
        kafkaTemplate.send("helloTopic","hello kafka");
    }


    public void sendEvent2() {
        User user  = User.builder().id(1209).phone("13345232").birthday(new Date()).build();
        String userJSON = JSONUtils.toJSON(user);
        kafkaTemplate.send("helloTopic",userJSON);
    }

    public void sendEvent3() {
        for (int i = 0; i < 25; i++) {
            User user  = User.builder().id(i).phone("13345232"+i).birthday(new Date()).build();
            String userJSON = JSONUtils.toJSON(user);
            kafkaTemplate.send("helloTopic","k" + i,userJSON);
        }

    }

    public void sendEvent4() {
        for (int i = 0; i < 125; i++) {
            User user  = User.builder().id(i).phone("13345232"+i).birthday(new Date()).build();
            String userJSON = JSONUtils.toJSON(user);
            kafkaTemplate.send("batchTopic","k" + i,userJSON);
        }
    }


    public void sendEvent5() {
        User user = User.builder().id(1).phone("13345232").birthday(new Date()).build();
        String userJSON = JSONUtils.toJSON(user);
        kafkaTemplate.send("intTopic","k1",userJSON);
    }

    public void sendEvent6() {
        User user = User.builder().id(1).phone("13345232").birthday(new Date()).build();
        String userJSON = JSONUtils.toJSON(user);
        kafkaTemplate.send("topicA","k1",userJSON);
    }

    public void sendEvent7() {
        User user = User.builder().id(1).phone("13345232").birthday(new Date()).build();
        String userJSON = JSONUtils.toJSON(user);
        kafkaTemplate.send("myTopic","k1",userJSON);
    }

    public void sendEvent8() {
        for (int i = 0; i < 100; i++) {
            User user = User.builder().id(1028+i).phone("242345235"+i).birthday(new Date()).build();
            String userJSON = JSONUtils.toJSON(user);
            kafkaTemplate.send("myTopic","k" + i,userJSON);
        }
    }

    public void sendEvent9() {
        for (int i = 0;i < 2;i++){
            User user = User.builder().id(1028 + i).phone("1370909090").birthday(new Date()).build();
            String userJSON = JSONUtils.toJSON(user);
            kafkaTemplate.send("offsetTopic","k" + i,userJSON);
        }
    }

    public void sendEvent10() {
        for (int i = 0;i < 2;i++){
            User user = User.builder().id(1028 + i).phone("1370909090").birthday(new Date()).build();
            String userJSON = JSONUtils.toJSON(user);
            kafkaTemplate.send("clustertopic","k" + i,userJSON);
        }
    }








}
