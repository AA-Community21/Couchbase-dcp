package com.tcket;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerApp {
    public static void main(String[] args) {
        // Kafka producer configuration
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");  // Kafka broker
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        try {
            // Send a test message to the Kafka topic "test-topic"
            String key = "key1";
            String value = "Hello Kafka!";
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", key, value);

            // Send the message asynchronously
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending message: " + exception.getMessage());
                } else {
                    System.out.println("Message sent successfully to topic: " + metadata.topic());
                }
            });

            // Sleep for a while to let the message be sent before closing
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();  // Close the producer
        }
    }
}