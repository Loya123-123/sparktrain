package com.yjz.spark.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor  implements ProducerInterceptor<String,String> {
    @Override
    public void configure(Map<String, ?> configs) {

    }
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 取出具体的数据 然后重新封装 value 再推回去
        return new ProducerRecord<String, String>(record.topic(),record.partition(),record.key(),System.currentTimeMillis()+","+record.value(),record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }


}
