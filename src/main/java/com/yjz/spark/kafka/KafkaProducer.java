package com.yjz.spark.kafka;



import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

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
        //
        properties.put("bootstrap.servers",KafkaProperties.BROKER_LIST);
        // 序列化
//        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 握手机制 0 不反馈， 1 写到本地 -1 检查所有 节点（较严格）
        properties.put("request.required.acks","1");
        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

    }

    @Override
    public void run() {
        int messageNo = 1 ;
        for (int i = 0; i <100; i++) {
            String message = "message_" + messageNo;
            // Integer.toString(messageNo) key
            // message value

            producer.send(new ProducerRecord<String, String>(topic,Integer.toString(messageNo),message));
            System.out.println("send: " + message);
            messageNo ++ ;
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
