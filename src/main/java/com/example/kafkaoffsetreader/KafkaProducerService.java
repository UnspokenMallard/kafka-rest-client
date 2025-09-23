package com.example.kafkaoffsetreader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    private final Map<String, KafkaProducer<String, byte[]>> producerPool = new ConcurrentHashMap<>();
    private ExecutorService producerExecutor;
    private Semaphore rateLimiter;
    private ScheduledExecutorService scheduler;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.producer.pool.size:10}")
    private int producerPoolSize;

    @Value("${kafka.producer.max.requests.per.second:1000}")
    private int maxRequestsPerSecond;

    @Value("${kafka.producer.client.id.prefix:kafka-producer}")
    private String clientIdPrefix;

    @Value("${kafka.producer.default.client.rack:}")
    private String defaultClientRack;

    @Value("${kafka.producer.acks:1}")
    private String producerAcks;

    @Value("${kafka.producer.batch.size:16384}")
    private int producerBatchSize;

    @Value("${kafka.producer.linger.ms:0}")
    private int producerLingerMs;

    private final ObjectMapper objectMapper;

    public KafkaProducerService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        if (initialized.compareAndSet(false, true)) {
            // Create dedicated thread pool for producers
            this.producerExecutor = Executors.newFixedThreadPool(
                producerPoolSize,
                new ThreadFactoryBuilder()
                    .setNameFormat("kafka-producer-thread-%d")
                    .setDaemon(true)
                    .build()
            );

            // Simple rate limiting
            this.rateLimiter = new Semaphore(maxRequestsPerSecond);

            // Schedule permit renewal
            this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("kafka-producer-rate-limiter-%d").setDaemon(true).build()
            );
            scheduler.scheduleAtFixedRate(() -> {
                rateLimiter.drainPermits();
                rateLimiter.release(maxRequestsPerSecond);
            }, 1, 1, TimeUnit.SECONDS);

            logger.info("KafkaProducerService initialized with pool size {} and max requests/sec {}", producerPoolSize, maxRequestsPerSecond);
        }
    }

    /**
     * Optimized async send with batching and streaming
     */
    @Async
    public CompletableFuture<Map<String, Object>> sendAsync(String topic, Map<String, Object> request, String clientRack) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Rate limiting
                if (!rateLimiter.tryAcquire(100, TimeUnit.MILLISECONDS)) {
                    logger.warn("Rate limit exceeded for Kafka producer");
                    throw new RuntimeException("Rate limit exceeded");
                }

                KafkaProducer<String, byte[]> producer = getOrCreateProducer(clientRack);

                @SuppressWarnings("unchecked")
                List<Map<String, Object>> records = (List<Map<String, Object>>) request.get("records");
                if (records == null) {
                    throw new IllegalArgumentException("'records' field is required and must be a list");
                }

                List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();

                for (Map<String, Object> record : records) {
                    ProducerRecord<String, byte[]> producerRecord = createProducerRecord(topic, record);

                    CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Kafka send failed", exception);
                            future.completeExceptionally(exception);
                        } else {
                            future.complete(metadata);
                        }
                    });
                    futures.add(future);
                }

                CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0])
                );

                return allFutures.thenApply(v -> {
                    List<Map<String, Object>> offsets = new ArrayList<>();
                    for (CompletableFuture<RecordMetadata> future : futures) {
                        try {
                            RecordMetadata metadata = future.get();
                            Map<String, Object> offsetInfo = new HashMap<>();
                            offsetInfo.put("partition", metadata.partition());
                            offsetInfo.put("offset", metadata.offset());
                            offsets.add(offsetInfo);
                        } catch (Exception e) {
                            logger.error("Error getting record metadata", e);
                            throw new RuntimeException(e);
                        }
                    }

                    Map<String, Object> response = new HashMap<>();
                    response.put("offsets", offsets);
                    return response;
                }).get();

            } catch (Exception e) {
                logger.error("Failed to send messages to Kafka", e);
                throw new RuntimeException("Failed to send messages to Kafka", e);
            }
        }, producerExecutor);
    }

    private KafkaProducer<String, byte[]> getOrCreateProducer(String rack) {
        String poolKey = (rack != null && !rack.isEmpty()) ? rack : (defaultClientRack != null ? defaultClientRack : "default");
        return producerPool.computeIfAbsent(poolKey, k -> {
            Properties props = createOptimizedProducerProperties(rack);
            return new KafkaProducer<>(props);
        });
    }

    private Properties createOptimizedProducerProperties(String rack) {
        Properties props = new Properties();

        // Basic config
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        props.put(ProducerConfig.ACKS_CONFIG, producerAcks);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, producerLingerMs);

        // Client identification
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-" + UUID.randomUUID().toString().substring(0, 8));

        if (rack != null && !rack.isEmpty() && !"default".equalsIgnoreCase(rack)) {
            props.put("client.rack", rack);
        } else if (defaultClientRack != null && !defaultClientRack.isEmpty()) {
            props.put("client.rack", defaultClientRack);
        }

        return props;
    }

    private ProducerRecord<String, byte[]> createProducerRecord(String topic, Map<String, Object> record) {
        try {
            Object keyObj = record.get("key");
            Object valueObj = record.get("value");
            Integer partition = (Integer) record.get("partition");

            String keyString = null;
            if (keyObj != null) {
                keyString = objectMapper.writeValueAsString(keyObj);
            }

            String valueString = objectMapper.writeValueAsString(valueObj);
            byte[] valueBytes = valueString.getBytes(StandardCharsets.UTF_8);

            if (partition != null && keyString != null) {
                return new ProducerRecord<>(topic, partition, keyString, valueBytes);
            } else if (partition != null) {
                return new ProducerRecord<>(topic, partition, null, valueBytes);
            } else if (keyString != null) {
                return new ProducerRecord<>(topic, keyString, valueBytes);
            } else {
                return new ProducerRecord<>(topic, valueBytes);
            }
        } catch (Exception e) {
            logger.error("Failed to create producer record", e);
            throw new RuntimeException("Failed to create producer record", e);
        }
    }

    @PreDestroy
    public void cleanup() {
        if (producerExecutor != null) {
            producerExecutor.shutdown();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
        producerPool.values().forEach(KafkaProducer::close);
        logger.info("KafkaProducerService cleaned up");
    }
}