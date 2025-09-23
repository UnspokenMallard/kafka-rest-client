package com.example.kafkaoffsetreader;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import javax.servlet.http.HttpServletRequest;

@RestController
public class KafkaReaderController {

    @Autowired
    private KafkaReaderService kafkaReaderService;

    @Autowired
    private KafkaConnectionPool connectionPool;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    /**
     * Main Kafka REST API endpoint with async high-performance implementation
     * Supports rack-aware consumer fetching and connection pooling
     * Includes consumer-based partition validation for performance
     */
    @GetMapping("/topics/{topic}/partitions/{partition}/messages")
    public CompletableFuture<ResponseEntity<Object>> readMessages(
        @PathVariable String topic,
        @PathVariable int partition,
        @RequestParam long offset,
        @RequestParam(defaultValue = "1") int count,
        @RequestParam(required = false) String clientRack,
        @RequestHeader(value = "Accept", defaultValue = "application/json") String acceptHeader,
        HttpServletRequest request
    ) {
        long start = System.currentTimeMillis();
        boolean useJsonFormat = acceptHeader.contains("application/vnd.kafka.json.v1+json");

        return kafkaReaderService.readAsync(topic, partition, offset, count, clientRack, useJsonFormat)
            .thenApply(messages -> {
                ResponseEntity<Object> response = ResponseEntity.ok((Object) messages);
                logRestStyle(request, start, 200, messages);
                return response;
            })
            .exceptionally(ex -> {
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                int status = HttpStatus.INTERNAL_SERVER_ERROR.value();
                Map<String, Object> error = new HashMap<>();
                // Kafka REST proxy style:
                error.put("error_code", 50002);
                error.put("message", cause.getMessage() != null ? cause.getMessage() : "Kafka error");
                logRestStyle(request, start, status, error);
                return ResponseEntity.status(status).body(error);
            });
    }

    private void logRestStyle(HttpServletRequest request, long start, int status, Object body) {
        int bytes = 0;
        try {
            bytes = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(body).length;
        } catch (Exception ignore) {}
        String now = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS")
                .format(java.time.LocalDateTime.now());
        String clientIp = request.getRemoteAddr();
        System.out.printf("[%s] INFO %s %s %s%s %d %d%n",
                now,
                clientIp,
                request.getMethod(),
                request.getRequestURI(),
                request.getQueryString() != null ? "?" + request.getQueryString() : "",
                status,
                bytes
        );
    }

    /**
     * Kafka REST API - Send messages to topic
     * POST /topics/{topic}
     */
    @PostMapping(
    value = "/topics/{topic}",
    consumes = {"application/json", "application/vnd.kafka.json.v2+json"}
    )   
    public CompletableFuture<ResponseEntity<Object>> sendMessages(
        @PathVariable String topic,
        @RequestBody Map<String, Object> request,
        @RequestParam(required = false) String clientRack,
        HttpServletRequest httpRequest
    ) {
        long start = System.currentTimeMillis();
        return kafkaProducerService.sendAsync(topic, request, clientRack)
            .thenApply(messages -> {
                ResponseEntity<Object> response = ResponseEntity.ok((Object) messages);
                logRestStyle(httpRequest, start, 200, messages);
                return response;
            })
            .exceptionally(ex -> {
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                int status = HttpStatus.INTERNAL_SERVER_ERROR.value();
                Map<String, Object> error = new HashMap<>();
                error.put("error_code", 50002);
                error.put("message", cause.getMessage() != null ? cause.getMessage() : "Kafka error");
                logRestStyle(httpRequest, start, status, error);
                return ResponseEntity.status(status).body(error);
            });
    }
        

    /**
     * Monitoring endpoint for connection pool stats
     */
    @GetMapping("/monitoring/pool-stats")
    public Map<String, Object> getPoolStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("pools", connectionPool.getPoolStats());
        stats.put("timestamp", System.currentTimeMillis());
        return stats;
    }
}