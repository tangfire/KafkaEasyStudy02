package com.fire.kafkaeasystudy02.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * 自定义消费者拦截器
 */
public class CustomConsumerInterceptor implements ConsumerInterceptor<String,String> {

    /**
     * 在消费消息之前执行
     * @param records records to be consumed by the client or records returned by the previous interceptors in the list.
     * @return
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        System.out.println("onConsume方法执行: " + records.count());
        return records;
    }

    /**
     * 消息拿到之后，提交offset之前执行
     * @param offsets A map of offsets by partition with associated metadata
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        System.out.println("onCommit方法执行: " + offsets);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
