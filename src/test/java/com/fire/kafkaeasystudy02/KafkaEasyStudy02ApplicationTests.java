package com.fire.kafkaeasystudy02;

import com.fire.kafkaeasystudy02.producer.EventProducer;
import jakarta.annotation.Resource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class KafkaEasyStudy02ApplicationTests {

    @Resource
    private EventProducer eventProducer;

    @Test
    void test01() {
        eventProducer.sendEvent();
    }

    @Test
    void test02() {
        eventProducer.sendEvent2();
    }

    @Test
    void test03() {
        eventProducer.sendEvent3();
    }

    @Test
    void test04() {
        eventProducer.sendEvent4();
    }

}
