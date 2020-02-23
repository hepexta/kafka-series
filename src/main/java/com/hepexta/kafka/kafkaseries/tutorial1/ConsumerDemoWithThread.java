package com.hepexta.kafka.kafkaseries.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static com.hepexta.kafka.kafkaseries.tutorial1.KafkaConfig.getConsumerProperties;

public class ConsumerDemoWithThread {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoCallback.class);
    private static final String GROUP_ID = "my-second-group";

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private void run() {
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerThread = new ConsumerThread(latch);

        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            LOG.info("myConsumerThread going to shutdown");
            ((ConsumerThread) myConsumerThread).shutdown();
            await(latch);
            LOG.info("Application is exited");
        }));

        await(latch);
    }

    private void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Application got interrupted ");
        }
        finally {
            LOG.info("Application is closing");
        }
    }

    private ConsumerDemoWithThread(){
        run();
    }

    public class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;

        public ConsumerThread(CountDownLatch latch) {
            this.latch = latch;
            this.consumer = new KafkaConsumer<>(getConsumerProperties(GROUP_ID));
            consumer.subscribe(Arrays.asList("first"));
        }

        @Override
        public void run() {
            try {
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
            } catch (WakeupException e){
                LOG.info("Received shutdown signal!");
            }
            finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shutdown(){
            // the wakeup method uses to interrupt consumer.poll()
            // it will thrown the exception WakeUpException
            consumer.wakeup();
        }
    }
}
