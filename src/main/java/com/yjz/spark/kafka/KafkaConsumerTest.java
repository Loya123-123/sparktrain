package com.yjz.spark.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

/**
 * kafka 消费者
 */
public class KafkaConsumerTest extends Thread{
    private String topic;

    public KafkaConsumerTest(String topic) {
        this.topic = topic;

    }
    private KafkaConsumer<String, String> createConnrctor(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers",KafkaProperties.BROKER_LIST);
        properties.put("enable.auto.commit", "true");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        properties.put("group.id",KafkaProperties.GROUP_ID);

        return new KafkaConsumer<>(properties);
    }

    @Override
    public void run() {
        KafkaConsumer<String, String> stringKafkaConsumer  = createConnrctor();
        stringKafkaConsumer.subscribe(Arrays.asList(KafkaProperties.TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = stringKafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.toString());
            }
        }
    }
}
