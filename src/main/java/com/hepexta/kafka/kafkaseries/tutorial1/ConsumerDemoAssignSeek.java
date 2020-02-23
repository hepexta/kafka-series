package com.hepexta.kafka.kafkaseries.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

import static com.hepexta.kafka.kafkaseries.tutorial1.KafkaConfig.getConsumerProperties;

public class ConsumerDemoAssignSeek {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoCallback.class);
    private static final String GROUP_ID = null;
    private static final String TOPIC = "first";

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties(GROUP_ID));

        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC, 0);
        long offsetToReadFrom = 15L;

        consumer.assign(Arrays.asList(partitionToReadFrom));

        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesToReadCounter = 0;
        boolean keepOnReading = true;

        while (keepOnReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesToReadCounter++;
                LOG.info(
                        "Key: "+record.key()+"\n"+
                        "Values: "+record.value()+"\n"+
                        "Partition: "+record.partition()+"\n"+
                        "Offset: "+record.offset()+"\n"+
                        "Topic: "+record.topic()
                );
                if (numberOfMessagesToReadCounter >= numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }
    }
}
