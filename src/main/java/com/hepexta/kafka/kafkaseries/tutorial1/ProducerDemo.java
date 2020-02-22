package com.hepexta.kafka.kafkaseries.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static com.hepexta.kafka.kafkaseries.tutorial1.KafkaConfig.getProducerProperties;

public class ProducerDemo {
    public static void main(String[] args) {
        Properties properties = getProducerProperties();
        KafkaProducer<String, String> producer = prepareProducer(properties);
        sendDataToKafka(producer);
    }

    private static void sendDataToKafka(KafkaProducer<String, String> producer) {
        producer.send(createProducerRecord());
        producer.close();
    }

    private static ProducerRecord<String, String> createProducerRecord() {
        return new ProducerRecord<String, String>("first", "hello world niggas");
    }

    private static KafkaProducer<String, String> prepareProducer(Properties properties) {
        return new KafkaProducer<String, String>(properties);
    }

}
