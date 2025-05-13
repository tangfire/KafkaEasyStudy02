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

    @Test
    void test05() {
        eventProducer.sendEvent5();
    }

    @Test
    void test06() {
        eventProducer.sendEvent6();
    }

    @Test
    void test07() {
        eventProducer.sendEvent7();
    }

    @Test
    void test08() {
        eventProducer.sendEvent8();
    }

    @Test
    void test09() {
        eventProducer.sendEvent9();
    }

}
