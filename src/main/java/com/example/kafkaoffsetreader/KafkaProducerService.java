package com.example.kafkaoffsetreader;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KafkaProducerService {

    private final Map<String, KafkaProducer<String, byte[]>> producerPool = new ConcurrentHashMap<>();

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.client.id:er-kafka-rest-producer}")
    private String clientIdPrefix;

    @Value("${kafka.client.rack:}")
    private String defaultClientRack;

    /**
     * Send messages to Kafka topic asynchronously
     */
    @Async
    public CompletableFuture<Map<String, Object>> sendAsync(String topic, Map<String, Object> request, String clientRack) {
        try {
            KafkaProducer<String, byte[]> producer = getProducer(clientRack);

            // Safe cast with validation
            Object recordsObj = request.get("records");
            if (!(recordsObj instanceof List)) {
                throw new IllegalArgumentException("'records' field must be a list");
            }
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> records = (List<Map<String, Object>>) recordsObj;
            Map<String, Object> response = new HashMap<>();
            List<Map<String, Object>> offsets = new ArrayList<>();

            for (Map<String, Object> record : records) {
                Object keyObj = record.get("key");  // Optional - can be any type
                Object valueObj = record.get("value");
                Integer partition = (Integer) record.get("partition");  // Optional

                // Handle key - JSON serialize like REST proxy
                String keyString = null;
                if (keyObj != null) {
                    keyString = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(keyObj);
                }

                // Handle value - JSON serialize like REST proxy
                String valueString = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(valueObj);
                byte[] valueBytes = valueString.getBytes(java.nio.charset.StandardCharsets.UTF_8);

                // Create ProducerRecord based on what's provided
                ProducerRecord<String, byte[]> producerRecord;
                if (partition != null && keyString != null) {
                    producerRecord = new ProducerRecord<>(topic, partition, keyString, valueBytes);
                } else if (partition != null) {
                    producerRecord = new ProducerRecord<>(topic, partition, null, valueBytes);
                } else if (keyString != null) {
                    producerRecord = new ProducerRecord<>(topic, keyString, valueBytes);
                } else {
                    producerRecord = new ProducerRecord<>(topic, valueBytes);
                }

                RecordMetadata metadata = producer.send(producerRecord).get();
                Map<String, Object> offsetInfo = new HashMap<>();
                offsetInfo.put("partition", metadata.partition());
                offsetInfo.put("offset", metadata.offset());
                offsetInfo.put("error_code", null);
                offsetInfo.put("error", null);
                offsets.add(offsetInfo);
            }

            response.put("offsets", offsets);
            return CompletableFuture.completedFuture(response);

        } catch (Exception e) {
            throw new RuntimeException("Failed to send messages to Kafka", e);
        }
    }

    private KafkaProducer<String, byte[]> getProducer(String rack) {
        String poolKey = rack != null ? rack : "default";
        return producerPool.computeIfAbsent(poolKey, k -> {
            Properties props = createProducerProperties(rack);
            return new KafkaProducer<>(props);
        });
    }

    private Properties createProducerProperties(String rack) {
        Properties props = new Properties();

        // Basic producer config
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        // Client identification and rack
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-" + UUID.randomUUID().toString().substring(0, 8));

        if (rack != null && !"default".equalsIgnoreCase(rack)) {
            props.put("client.rack", rack);
        } else if (defaultClientRack != null && !defaultClientRack.isEmpty()) {
            props.put("client.rack", defaultClientRack);
        }

        return props;
    }
}
