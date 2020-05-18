package com.yjz.spark.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CountInterceptor implements ProducerInterceptor<String,String>{
    int success ;
    int error ;
    @Override
    public void configure(Map<String, ?> configs) {

    }
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            success ++ ;
        }else {
            error ++ ;
        }
    }

    @Override
    public void close() {
        System.out.println("success "+ success);
        System.out.println("error " + error);
    }

}
