package com.hepexta.kafka.kafkaseries.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

import static com.hepexta.kafka.kafkaseries.tutorial1.KafkaConfig.getConsumerProperties;

public class ConsumerDemo {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoCallback.class);

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties());

        consumer.subscribe(Arrays.asList("first"));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                LOG.info(
                        "Key: "+record.key()+"\n"+
                        "Values: "+record.value()+"\n"+
                        "Partition: "+record.partition()+"\n"+
                        "Offset: "+record.offset()+"\n"+
                        "Topic: "+record.topic()+"\n"
                );
            }
        }
    }
}
