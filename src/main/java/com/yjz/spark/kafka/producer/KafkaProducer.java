package com.yjz.spark.kafka.producer;


import com.yjz.spark.kafka.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Properties;


/**
 * kafka生产者
 */
public class KafkaProducer extends Thread {
    private String topic ;

    Producer<String,String> producer ;
    public KafkaProducer(String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        // KAFKA 集群 "bootstrap.servers"
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaProperties.BROKER_LIST);
        // 序列化 KEY value
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // ACK 应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数 
        properties.put("retries", 3);
        //批次大小 （字节）
        properties.put("batch.size", 16384);
        //等待时间 （毫秒）
        properties.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小(字节)  32M
        properties.put("buffer.memory", 33554432);

        // 添加拦截器
        ArrayList<Object> interceptors = new ArrayList<>();
        interceptors.add("com.yjz.spark.kafka.interceptor.CountInterceptor");
        interceptors.add("com.yjz.spark.kafka.interceptor.TimeInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        // 握手机制 0 不反馈， 1 写到本地 -1 检查所有 节点（较严格）
        properties.put("request.required.acks","1");
        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

    }

    @Override
    public void run() {
        int messageNo = 1 ;
        for (int i = 0; i <100; i++) {
            final String message = "message_" + messageNo;
            // Integer.toString(messageNo) key
            // message value
            producer.send(new ProducerRecord<String, String>(topic, Integer.toString(messageNo), message), (recordMetadata, e) -> {
                if (e == null){
                    int partition = recordMetadata.partition();
                    long offset = recordMetadata.offset();
                    System.out.println(partition +"---" +offset);
                }
            });
            System.out.println("send: " + message);
            messageNo ++ ;
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}
