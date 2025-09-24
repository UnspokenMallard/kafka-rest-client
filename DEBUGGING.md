### Debugging Environment Deployment Guide

This project is a Java 11 Spring Boot service with Kafka client dependencies. Use the following steps to deploy a reliable local debugging environment and enable remote debugging when needed.

---

## 1) Prerequisites
- **Java**: AdoptOpenJDK/OpenJDK 11
- **Maven**: 3.8+
- **Kafka**: Local brokers reachable at `localhost:9092,9094,9096` (adjust as needed)
- Optional: Docker (if running Kafka locally via containers)

Verify tools:
```bash
java -version
mvn -v
```

---

## 2) Build with patched Kafka client
The build installs the patched Kafka client JAR from `lib/` into your local Maven repo automatically.

```bash
mvn clean package -DskipTests
```

If the JAR is missing or renamed, ensure the file exists at:
`lib/kafka-clients-4.2.0-follower-fetch.jar`

---

## 3) Start in Development Mode (Hot Reload + Local Debugging)

### Option A: Spring Boot Maven Plugin with Remote Debugging
Run the app with JDWP enabled to attach a debugger (port 5005 by default):

```bash
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
```

Attach your IDE debugger to `localhost:5005`.

### Option B: Run the shaded JAR with Remote Debugging

```bash
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 \
     -jar target/kafka-offset-reader-1.0.0.jar
```

---

## 4) Configuration for Debug Sessions

The app loads configuration in this order: runtime override (query params) → external file → `application.properties`.

- Default dev settings are in `src/main/resources/application.properties`.
- External config sample is at `etc/kafka-rest/er-kafka-rest.properties`.

Run with external config (relative to the JAR):
```bash
java -jar target/kafka-offset-reader-1.0.0.jar ./etc/kafka-rest/er-kafka-rest.properties
```

Key properties to adjust for debugging:
- `server.port=8080` (change if port conflicts)
- `kafka.bootstrap.servers=...` (point to your Kafka cluster)
- `kafka.client.rack=zone-x` (to test rack-aware behavior)
- Logging levels in `application.properties` (set to DEBUG when needed)

---

## 5) Useful Debug/Health Endpoints

- Read messages:
```bash
curl "http://localhost:8080/topics/{topic}/partitions/{partition}/messages?offset=0&count=3&clientRack=zone-a"
```

- Produce messages:
```bash
curl -X POST "http://localhost:8080/topics/{topic}" \
  -H "Content-Type: application/json" \
  -d '{"records":[{"key":"k1","value":{"msg":"hello"},"partition":0}]}'
```

- Pool stats:
```bash
curl "http://localhost:8080/monitoring/pool-stats"
```

---

## 6) Common Debugging Scenarios

- Breakpoints in `KafkaReaderService.read` for offset validation and polling loop.
- Inspect producer flow in `KafkaProducerService.sendAsync` for batching and metadata.
- Verify rack selection in `KafkaConnectionPool.createConsumer` (`client.rack`).
- External config loading in `KafkaOffsetReaderApplication.main`.

---

## 7) Troubleshooting

- Port already in use: change `server.port` or stop the conflicting process.
- Kafka connectivity errors: verify `kafka.bootstrap.servers`, security config, and broker reachability.
- Partition out of range: ensure topic/partition exists and offsets are valid.
- Missing patched Kafka client: confirm `lib/kafka-clients-4.2.0-follower-fetch.jar` exists; rebuild.

Increase log verbosity temporarily by adding to `src/main/resources/application.properties`:
```properties
logging.level.com.example.kafkaoffsetreader=DEBUG
logging.level.org.apache.kafka=DEBUG
```

---

## 8) Remote Debugging in Container/Server

Expose port 5005 and run with JDWP:
```bash
JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005" \
java -jar kafka-offset-reader-1.0.0.jar ./etc/kafka-rest/er-kafka-rest.properties
```

Ensure network/firewall rules allow inbound 5005 from your IDE.

---

## 9) Quick Sanity Checks

```bash
mvn -q -DskipTests initialize && echo OK: Maven initialize
mvn -q -DskipTests clean compile && echo OK: Compiled
test -f lib/kafka-clients-4.2.0-follower-fetch.jar && echo OK: Patched client present || echo MISSING: patched client
```

