package com.yjz.spark.kafka;

public class KafkaClientAPP {
    public static void main(String[] args) {
        // new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumerTest(KafkaProperties.TOPIC).start();
    }
}
