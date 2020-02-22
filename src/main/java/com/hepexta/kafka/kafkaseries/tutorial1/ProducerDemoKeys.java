package com.hepexta.kafka.kafkaseries.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.hepexta.kafka.kafkaseries.tutorial1.KafkaConfig.getProducerProperties;

public class ProducerDemoKeys {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) {
        Properties properties = getProducerProperties();
        KafkaProducer<String, String> producer = prepareProducer(properties);
        sendDataToKafka(producer);
    }

    private static void sendDataToKafka(KafkaProducer<String, String> producer) {
        for (int i=0; i<10; i++){
            producer.send(createProducerRecord(i), producingCallback());
        }

        producer.close();
    }

    private static Callback producingCallback() {
        return (recordMetadata, e) -> {
                /* executes every time a record is successfully sent or exception is thrown */
                if (e == null){
                    LOG.info("Received new metadata \n"+
                    "Topic:"+recordMetadata.topic()+"\n"+
                    "Partition:"+recordMetadata.partition()+"\n"+
                    "Offset:"+recordMetadata.offset()+"\n"+
                    "Timestamp:"+recordMetadata.timestamp()+"\n"
                    );
                }
                else {
                    LOG.error("Error while producing", e);
                }
            };
    }

    private static ProducerRecord<String, String> createProducerRecord(int i) {
        String key = String.format("key_%d", i);
        LOG.info("Key: "+key);

        // SAME KEY ALWAYS GOES TO THE SAME PARTITION
        return new ProducerRecord<String, String>("first", key, String.format("hello world %d", i));
    }

    private static KafkaProducer<String, String> prepareProducer(Properties properties) {
        return new KafkaProducer<String, String>(properties);
    }
}
