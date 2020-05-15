package com.yjz.spark.kafka;

import com.yjz.spark.kafka.consumer.KafkaConsumerTest;
import com.yjz.spark.kafka.producer.KafkaProducer;

public class KafkaClientAPP {
    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumerTest(KafkaProperties.TOPIC).start();
    }
}
