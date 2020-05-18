package com.yjz.spark.kafka.consumer;

import com.yjz.spark.kafka.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
        // 创建集群配置
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaProperties.BROKER_LIST);
        // 开启自动提交  除了自动提交，还可以切换为手动提交的两种方式 ，同步和异步。
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自动提交延迟
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        // 重置消费者 的offset 默认 latest
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // key,value 反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,KafkaProperties.GROUP_ID);
        // 创建消费者
        return new KafkaConsumer<>(properties);
    }

    @Override
    public void run() {
        KafkaConsumer<String, String> stringKafkaConsumer  = createConnrctor();
        // 订阅 topics
        stringKafkaConsumer.subscribe(Arrays.asList(KafkaProperties.TOPIC));
        while (true) {
            // 获取数据
            ConsumerRecords<String, String> records = stringKafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.toString());
            }
        }
    }
}
