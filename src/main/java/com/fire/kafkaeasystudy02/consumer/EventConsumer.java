package com.fire.kafkaeasystudy02.consumer;

import com.fire.kafkaeasystudy02.model.User;
import com.fire.kafkaeasystudy02.util.JSONUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class EventConsumer {
    // 采用监听的方式接收事件(消息、数据)
//    @KafkaListener(topics = {"helloTopic"},groupId = "helloGroup")
    public void onEvent(@Payload String event,
                        @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
//                        @Header(value = KafkaHeaders.RECEIVED_KEY) String key,
                        @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                        ConsumerRecord<String, String> record) {
//        System.out.println("读取的事件："+event + ", topic:"+topic+", key:"+key+", partition:"+partition);
        System.out.println("读取的事件1："+event + ", topic:"+topic+", partition: "+partition);
        System.out.println("读取的事件1："+record.toString());

    }


    // 采用监听的方式接收事件(消息、数据)
//    @KafkaListener(topics = {"helloTopic"},groupId = "helloGroup")
    public void onEvent2(String userJSON,
                         @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
//                        @Header(value = KafkaHeaders.RECEIVED_KEY) String key,
                         @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                         ConsumerRecord<String, String> record) {
//        System.out.println("读取的事件："+event + ", topic:"+topic+", key:"+key+", partition:"+partition);
        User user = JSONUtils.toBean(userJSON,User.class);
        System.out.println("读取的事件2："+user + ", topic:"+topic+", partition: "+partition);
        System.out.println("读取的事件2："+record.toString());

    }

    // 采用监听的方式接收事件(消息、数据)
//    @KafkaListener(topics = {"${kafka.topic.name}"},groupId = "${kafka.consumer.group}")
    public void onEvent3(String userJSON,
                         @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
//                        @Header(value = KafkaHeaders.RECEIVED_KEY) String key,
                         @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                         ConsumerRecord<String, String> record) {
//        System.out.println("读取的事件："+event + ", topic:"+topic+", key:"+key+", partition:"+partition);
        User user = JSONUtils.toBean(userJSON,User.class);
        System.out.println("读取的事件3："+user + ", topic:"+topic+", partition: "+partition);
        System.out.println("读取的事件3："+record.toString());

    }

    // 采用监听的方式接收事件(消息、数据)
//    listener:
//        # 开启消息监听的手动确认模式
//        ack-mode: manual
//    @KafkaListener(topics = {"${kafka.topic.name}"},groupId = "${kafka.consumer.group}")
    public void onEvent4(String userJSON,
                         @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
//                        @Header(value = KafkaHeaders.RECEIVED_KEY) String key,
                         @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                         ConsumerRecord<String, String> record,
                         Acknowledgment ack) {
//        System.out.println("读取的事件："+event + ", topic:"+topic+", key:"+key+", partition:"+partition);
        // 收到消息后，处理业务
        try {
            User user = JSONUtils.toBean(userJSON, User.class);
            System.out.println("读取的事件4：" + user + ", topic:" + topic + ", partition: " + partition);
            System.out.println("读取的事件4：" + record.toString());
            // 业务处理完成，给kafka服务器确认
            ack.acknowledge(); // 手动确认消息，就是告诉kafka服务器，该消息我已经收到了，默认情况下kafka是自动确认
        }catch (Exception e){
            e.printStackTrace();
        }


    }


//    @KafkaListener(
//            groupId = "${kafka.consumer.group}",
//            topicPartitions = {
//                    @TopicPartition(topic = "${kafka.topic.name}",
//                            partitions = {"0","1","2"},
//                            partitionOffsets = {
//                                    @PartitionOffset(partition = "3", initialOffset = "3"),
//                                    @PartitionOffset(partition = "4", initialOffset = "3")})
//
//            }
//    )
    public void onEvent5(String userJSON,
                         @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
//                        @Header(value = KafkaHeaders.RECEIVED_KEY) String key,
                         @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                         ConsumerRecord<String, String> record,
                         Acknowledgment ack) {
//        System.out.println("读取的事件："+event + ", topic:"+topic+", key:"+key+", partition:"+partition);
        // 收到消息后，处理业务
        try {
            User user = JSONUtils.toBean(userJSON, User.class);
            System.out.println("读取的事件5：" + user + ", topic:" + topic + ", partition: " + partition);
            // 业务处理完成，给kafka服务器确认
            ack.acknowledge(); // 手动确认消息，就是告诉kafka服务器，该消息我已经收到了，默认情况下kafka是自动确认
        }catch (Exception e){
            e.printStackTrace();
        }


    }


//    @KafkaListener(topics = {"batchTopic"},groupId = "batchGroup")
    public void onEvent6(List<ConsumerRecord<String, String>> records) {
        System.out.println("批量消费,records.size()="+records.size()+",recods="+records);
    }

    @KafkaListener(topics = {"intTopic"},groupId = "intGroup",containerFactory = "ourKafkaListenerContainerFactory")
    public void onEvent7(ConsumerRecord<String, String> record) {
        System.out.println("消息消费7 record="+record);
    }

    @KafkaListener(topics = {"topicA"},groupId = "aGroup")
    @SendTo(value = "topicB")
    public String onEvent8(ConsumerRecord<String, String> record) {
        System.out.println("消息A消费，record" + record);
        return record.value() + "--forward message";
    }


    @KafkaListener(topics = {"topicB"},groupId = "bGroup")
    public void onEvent9(ConsumerRecord<String, String> record) {
        System.out.println("消息B消费，record" + record);

    }


//    @KafkaListener(topics = {"myTopic"},groupId = "myGroup")
    public void onEvent10(ConsumerRecord<String, String> record) {
        System.out.println("消息消费10，record" + record);
    }

//    @KafkaListener(topics = {"myTopic"},groupId = "myGroup",concurrency = "3")
    public void onEvent11(ConsumerRecord<String, String> record) {
        System.out.println( Thread.currentThread().getId()+ "->消息消费11，record" + record);
    }

    @KafkaListener(topics = {"myTopic"},groupId = "myGroup",concurrency = "3",containerFactory = "ourKafkaListenerContainerFactory")
    public void onEvent12(ConsumerRecord<String, String> record) {
        System.out.println( Thread.currentThread().getId()+ "->消息消费12，record" + record);
    }

    @KafkaListener(topics = {"offsetTopic"},groupId = "offsetGroup")
    public void onEvent13(ConsumerRecord<String, String> record) {
        System.out.println(Thread.currentThread().getId() + "->消息消费13,record" + record);
    }


    // 采用监听的方式接收事件(消息、数据)
    @KafkaListener(topics = {"clustertopic"},groupId = "clustergroup")
    public void onEvent01(String event) {
        System.out.println("读取的事件："+event);
    }



}
