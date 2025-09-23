package com.example.kafkaoffsetreader;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Kafka Connection Pool for High-Performance Request Handling
 * 
 * Maintains a pool of reusable KafkaConsumer instances to handle
 * high-volume concurrent requests efficiently.
 * 
 * Features:
 * - Connection pooling to avoid TCP overhead
 * - Thread-safe consumer management
 * - Rack-aware consumer separation
 * - Automatic connection cleanup
 */
@Component
public class KafkaConnectionPool {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaConnectionPool.class);
    
    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;
    
    @Value("${kafka.client.rack:}")
    private String defaultClientRack;

    @Value("${kafka.enable.auto.commit:false}")
    private String enableAutoCommit;

    @Value("${kafka.auto.offset.reset:earliest}")
    private String autoOffsetReset;

    @Value("${kafka.fetch.max.wait.ms:500}")
    private String fetchMaxWaitMs;

    @Value("${kafka.fetch.min.bytes:1}")
    private String fetchMinBytes;

    @Value("${kafka.max.poll.records:500}")
    private String maxPollRecords;

    @Value("${kafka.request.timeout.ms:10000}")
    private String requestTimeoutMs;

    @Value("${kafka.session.timeout.ms:20000}")
    private String sessionTimeoutMs;

    @Value("${kafka.heartbeat.interval.ms:5000}")
    private String heartbeatIntervalMs;

    @Value("${kafka.connections.max.idle.ms:300000}")
    private String connectionsMaxIdleMs;

    @Value("${kafka.metadata.max.age.ms:180000}")
    private String metadataMaxAgeMs;
    
    
    // Pool configuration
    private static final int MAX_POOL_SIZE = 50;
    private static final int MIN_POOL_SIZE = 15;
    // private static final long CONSUMER_TIMEOUT_MS = 30000; // 30 seconds (reserved for future use)
    
    // Separate pools for different rack configurations
    private final Map<String, BlockingQueue<KafkaConsumer<String, byte[]>>> consumerPools = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> poolLocks = new ConcurrentHashMap<>();
    
    @PostConstruct
    public void initializePools() {
        logger.info("Initializing Kafka connection pools...");

        // Always create the default pool
        createPool("default");

        // Also prewarm the effective rack from external/application config
        String effective = getEffectiveRack();
        if (!"default".equalsIgnoreCase(effective)) {
            createPool(effective);
        }

        logger.info("Kafka connection pools initialized with {} pools (default + effective rack)", consumerPools.size());
    }
    
    @PreDestroy
    public void cleanup() {
        logger.info("Cleaning up Kafka connection pools...");
        
        consumerPools.forEach((rack, pool) -> {
            while (!pool.isEmpty()) {
                try {
                    KafkaConsumer<String, byte[]> consumer = pool.poll();
                    if (consumer != null) {
                        consumer.close();
                    }
                } catch (Exception e) {
                    logger.warn("Error closing consumer: {}", e.getMessage());
                }
            }
        });
        
        consumerPools.clear();
        poolLocks.clear();
        
        logger.info("Kafka connection pools cleaned up");
    }
    
    /**
     * Get a consumer from the pool for the specified rack
     */
    public KafkaConsumer<String, byte[]> borrowConsumer(String clientRack) {
        String effectiveRack = clientRack != null ? clientRack : getEffectiveRack();
        
        BlockingQueue<KafkaConsumer<String, byte[]>> pool = consumerPools.get(effectiveRack);
        if (pool == null) {
            // Create pool on-demand
            createPool(effectiveRack);
            pool = consumerPools.get(effectiveRack);
        }
        
        KafkaConsumer<String, byte[]> consumer = pool.poll();
        if (consumer == null) {
            // Pool is empty, create new consumer
            consumer = createConsumer(effectiveRack);
            logger.debug("Created new consumer for rack: {} (pool was empty)", effectiveRack);
        } else {
            logger.debug("Reused consumer from pool for rack: {}", effectiveRack);
        }
        
        return consumer;
    }
    
    /**
     * Return a consumer to the pool
     */
    public void returnConsumer(KafkaConsumer<String, byte[]> consumer, String clientRack) {
        String effectiveRack = clientRack != null ? clientRack : getEffectiveRack();
        
        BlockingQueue<KafkaConsumer<String, byte[]>> pool = consumerPools.get(effectiveRack);
        if (pool != null && pool.size() < MAX_POOL_SIZE) {
            pool.offer(consumer);
            logger.debug("Returned consumer to pool for rack: {}", effectiveRack);
        } else {
            // Pool is full or doesn't exist, close the consumer
            consumer.close();
            logger.debug("Closed excess consumer for rack: {}", effectiveRack);
        }
    }
    
    /**
     * Create a new pool for the specified rack
     */
    private void createPool(String rack) {
        consumerPools.computeIfAbsent(rack, r -> {
            logger.info("Creating new consumer pool for rack: {}", r);
            BlockingQueue<KafkaConsumer<String, byte[]>> pool = new LinkedBlockingQueue<>();
            
            // Pre-populate with minimum consumers
            for (int i = 0; i < MIN_POOL_SIZE; i++) {
                pool.offer(createConsumer(r));
            }
            
            poolLocks.put(r, new ReentrantLock());
            logger.info("Created consumer pool for rack {} with {} pre-allocated consumers", r, MIN_POOL_SIZE);
            return pool;
        });
    }
    
    /**
     * Create a new KafkaConsumer instance
     */
    private KafkaConsumer<String, byte[]> createConsumer(String clientRack) {
        Properties props = new Properties();

        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "pooled-consumer-group-" + clientRack);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // From er-kafka-rest.properties
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        props.put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdleMs);
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, metadataMaxAgeMs);

        // Client ID: use default prefix, always append unique suffix to avoid collisions
        String clientIdPrefix = ("pooled-consumer-" + clientRack);
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientIdPrefix + "-" + UUID.randomUUID());

        // Rack-aware configuration
        if (clientRack != null && !"default".equalsIgnoreCase(clientRack)) {
            // Explicit pool rack wins
            props.put(CommonClientConfigs.CLIENT_RACK_CONFIG, clientRack);
        }

        logger.info("Creating consumer for rack='{}' (client.rack='{}')", clientRack,
                props.getProperty(CommonClientConfigs.CLIENT_RACK_CONFIG, ""));
        return new KafkaConsumer<>(props);
    }

    /**
     * Get effective rack from configuration hierarchy
     */
    private String getEffectiveRack() {
        return defaultClientRack.isEmpty() ? "default" : defaultClientRack;
    }

    /**
     * Get pool statistics for monitoring
     */
    public Map<String, Object> getPoolStats() {
        Map<String, Object> stats = new HashMap<>();
        
        consumerPools.forEach((rack, pool) -> {
            Map<String, Object> poolStats = new HashMap<>();
            poolStats.put("available", pool.size());
            poolStats.put("maxSize", MAX_POOL_SIZE);
            poolStats.put("minSize", MIN_POOL_SIZE);
            stats.put(rack, poolStats);
        });
        
        return stats;
    }

    /**
     * Get Kafka properties for AdminClient creation
     */
    public Properties getAdminProperties() {
        Properties props = new Properties();
        // Use Spring Boot injected property
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Client ID for AdminClient
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, "admin-" + UUID.randomUUID());
        // No rack config for AdminClient
        return props;
    }
}
