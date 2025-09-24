# Kafka Offset Reader

A high-performance Spring Boot application that provides REST API access to Kafka messages with rack-aware optimization and connection pooling.

## Features

- **100% Kafka REST Proxy Compatible** - Identical response format and field ordering
- **Follower message fetching** for optimized network performance
- **Connection pooling** with pre-allocated consumers and producers
- **Async processing** for high throughput operations
- **External configuration** support via ER properties
- **Real-time monitoring** of connection pools
- **Message production** with batch optimization
- **Performance tuning** based on ER Kafka REST configurations

## API Endpoints

### Read Messages
```
GET /topics/{topic}/partitions/{partition}/messages
```
**Parameters:**
- `offset` (required): Starting offset to read from
- `count` (optional): Number of messages to retrieve (default: 10, max: 1000)
- `clientRack` (optional): Preferred rack for rack-aware fetching

**Example:**
```bash
curl "http://localhost:8080/topics/my-topic/partitions/0/messages?offset=100&count=5&clientRack=zone-a"
```

### Produce Messages
```
POST /topics/{topic}
```
**Parameters:**
- `clientRack` (optional): Rack preference for producer optimization

**Request Body:**
```json
{
  "records": [
    {
      "key": "message-key",
      "value": "message content",
      "partition": 0
    }
  ]
}
```

**Example:**
```bash
curl -X POST "http://localhost:8080/topics/my-topic" \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      {
        "key": "user-123",
        "value": "Hello Kafka!",
        "partition": 0
      }
    ]
  }'
```

### Get Partition Offsets
```
GET /topics/{topic}/partitions/{partition}/offsets
```
**Parameters:**
- `clientRack` (optional): Rack preference for optimization

**Response:**
```json
{
  "beginning_offset": 0,
  "end_offset": 1234
}
```

### Monitoring
```
GET /monitoring/pool-stats
```
Returns connection pool statistics and health information.


## Local Setup

### Prerequisites

- Java 11 (OpenJDK/Temurin)
- Maven 3.8+
- A reachable Kafka broker/cluster

Check tools:
```bash
java -version
mvn -v
```

### Kafka for Local Development

You can either point to an existing Kafka cluster or run Kafka locally.

- Option A (quick start): Update `src/main/resources/application.properties` to use a single local broker:
```properties
kafka.bootstrap.servers=localhost:9092
```

Run a single-broker Kafka via Docker:
```bash
cat > docker-compose.kafka.yml <<'YAML'
services:
  kafka:
    image: bitnami/kafka:3.7
    environment:
      - KAFKA_ENABLE_KRAFT=yes
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093
      - KAFKA_CFG_NODE_ID=1
    ports:
      - "9092:9092"
YAML
docker compose -f docker-compose.kafka.yml up -d
```

- Option B: Use your existing cluster and keep `kafka.bootstrap.servers=localhost:9092, localhost:9094, localhost:9096` or point to your brokers.

### Build and Run

```bash
# Build (patched Kafka client JAR auto-installs)
mvn clean package -DskipTests

# Run in development
mvn spring-boot:run

# Or run the JAR
java -jar target/kafka-offset-reader-1.0.0.jar
```

To run with an external config on Linux/macOS:
```bash
java -jar target/kafka-offset-reader-1.0.0.jar ./etc/kafka-rest/er-kafka-rest.properties
```

For remote debugging and troubleshooting see `DEBUGGING.md`.

### Running the Application

#### Option 1: Default Configuration
```bash
# Compile and run with built-in settings
mvn clean package -DskipTests
java -jar target/kafka-offset-reader-1.0.0.jar
```
*Uses configuration from `etc/kafka-rest/er-kafka-rest.properties`*

#### Option 2: External Configuration File
```bash
# Run with external properties file
java -jar target/kafka-offset-reader-1.0.0.jar /path/to/your/config.properties
```

**Example with relative path:**
```bash
# Place config file relative to JAR location
java -jar target/kafka-offset-reader-1.0.0.jar .\etc\kafka-rest\er-kafka-rest.properties
```

**Example with absolute path:**
```bash
# Use absolute path to config file
java -jar target/kafka-offset-reader-1.0.0.jar C:\kafka-configs\production.properties
```

### Maven Development
```bash
# Compile
mvn initialize
mvn clean compile

# Run in development
mvn spring-boot:run
```

## Performance Features

- **15 pre-allocated consumers** per rack for instant message retrieval
- **Producer pooling** with rack-aware routing
- **Optimized fetch settings** from ER configuration
- **Async thread pool** (10-50 threads) for concurrent operations
- **Connection reuse** to minimize TCP overhead

## Architecture

This application significantly outperforms the standard Kafka REST Proxy through:

1. **Rack-aware optimization**: Consumers fetch from same-rack brokers
2. **Connection pooling**: Eliminates connection setup overhead
3. **ER configuration**: Uses enterprise-grade Kafka settings
4. **Async processing**: Non-blocking operations for high throughput
5. **Producer efficiency**: Batching and rack-aware message routing

## Rack-Aware Benefits

- **Reduced latency** - Read from local replicas
- **Lower network costs** - Avoid cross-zone traffic  
- **Better load distribution** - Spread load across brokers
- **High availability** - Automatic failover to other racks


### Application Logs Analysis

The application provides detailed logging showing rack-aware behavior:

```log
2025-01-09 12:15:23 INFO  - External configuration loaded: kafka.client.rack=zone-b
2025-01-09 12:15:30 INFO  - Reading from topic: test-topic, partition: 0, rack: zone-b
2025-01-09 12:15:30 DEBUG - Consumer created with rack preference: zone-b
2025-01-09 12:15:30 DEBUG - Kafka client attempting connection to zone-b broker first
2025-01-09 12:15:30 INFO  - Successfully read 3 messages from rack zone-b
2025-01-09 12:15:31 DEBUG - Connection pattern: zone-b (preferred) -> zone-a (failover)
```

## Configuration

### Configuration Hierarchy

The application supports a comprehensive configuration hierarchy for production deployment flexibility:

1. **Runtime Override** (Highest Priority) - Via REST API parameters
2. **External Configuration** - `etc/kafka-rest/er-kafka-rest.properties` (relative to JAR)
3. **Application Properties** - `src/main/resources/application.properties`
4. **Default Values** (Lowest Priority) - Hardcoded fallbacks

### External Configuration Setup

For production deployment, create an external configuration file:

**File Location**: `etc/kafka-rest/er-kafka-rest.properties` (relative to JAR file)

```properties
# Kafka Connection Configuration
kafka.bootstrap.servers=prod-kafka-1:9092,prod-kafka-2:9092,prod-kafka-3:9092
kafka.client.dns.lookup=use_all_dns_ips
kafka.client.timeout.ms=30000
kafka.client.request.timeout.ms=60000

# Rack-Aware Configuration
kafka.client.rack=zone-b

# Consumer Configuration
kafka.consumer.group.id=kafka-offset-reader
kafka.consumer.auto.offset.reset=earliest
kafka.consumer.enable.auto.commit=false

# Connection Pool Settings
kafka.consumer.max.poll.records=500
kafka.consumer.session.timeout.ms=30000
kafka.consumer.heartbeat.interval.ms=3000

# Security Configuration (if needed)
# kafka.security.protocol=SASL_SSL
# kafka.sasl.mechanism=PLAIN
# kafka.sasl.jaas.config=...
```

### Application Properties (Built-in)

Default configuration in `src/main/resources/application.properties`:

```properties
# Server Configuration  
server.port=8080

# Kafka Cluster Connection (Development)
kafka.bootstrap.servers=localhost:9092, localhost:9094, localhost:9096
kafka.client.dns.lookup=use_all_dns_ips

# Rack-Aware Follower Fetching Configuration
kafka.client.rack=zone-c

# Enable debug logging
logging.level.org.apache.kafka.clients.consumer=DEBUG
```

### Runtime Override

Override any configuration per request using REST API parameters:

```bash
# Override rack selection to zone-a
curl "http://localhost:8080/topics/test-topic/partitions/0/messages?offset=0&count=3&clientRack=zone-a"

# Override rack selection to zone-b  
curl "http://localhost:8080/topics/test-topic/partitions/0/messages?offset=0&count=3&clientRack=zone-b"

# Use configured default (external config → application.properties → zone-c)
curl "http://localhost:8080/topics/test-topic/partitions/0/messages?offset=0&count=3"
```

### Configuration Validation

The application logs the configuration hierarchy at startup:

```log
2025-01-09 10:15:23 INFO  - External configuration loaded from: /opt/kafka-services/etc/kafka-rest/er-kafka-rest.properties
2025-01-09 10:15:23 INFO  - Configuration hierarchy: Runtime → External → Application → Defaults
2025-01-09 10:15:23 INFO  - Active kafka.client.rack: zone-b (source: external)
2025-01-09 10:15:23 INFO  - Active kafka.bootstrap.servers: prod-kafka-1:9092,prod-kafka-2:9092,prod-kafka-3:9092 (source: external)
```

## 🛠️ Technical Implementation

### Custom Kafka Client Patch

This project uses a **patched Kafka client** (`kafka-clients-4.2.0-follower-fetch.jar`) that adds:

- **`client.rack` property**: Specifies which rack the client belongs to for rack-aware replica selection
- **Runtime override capability**: Allows changing rack selection per consumer
- **Enhanced logging**: Shows which rack is being used for consumption

### Maven Configuration

The `pom.xml` includes an automatic installation plugin:

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-install-plugin</artifactId>
  <executions>
    <execution>
      <id>install-patched-kafka</id>
      <phase>validate</phase>
      <goals>
        <goal>install-file</goal>
      </goals>
      <configuration>
        <file>lib/kafka-clients-4.2.0-follower-fetch.jar</file>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>4.2.0-follower-fetch</version>
        <packaging>jar</packaging>
      </configuration>
    </execution>
  </executions>
</plugin>
```

This ensures the patched JAR is automatically installed to your local Maven repository during the build process.

## 📁 Project Structure

```
kafka-offset-reader/
├── lib/
│   └── kafka-clients-4.2.0-follower-fetch.jar  # Patched Kafka client for rack-aware fetching
├── src/main/java/com/example/kafkaoffsetreader/
│   ├── KafkaOffsetReaderApplication.java        # Spring Boot main class with async configuration
│   ├── KafkaReaderController.java               # REST API endpoints (GET/POST/monitoring)
│   ├── KafkaReaderService.java                  # Kafka read operations with connection pooling
│   ├── KafkaProducerService.java                # Kafka producer service with rack-aware routing
│   ├── KafkaConnectionPool.java                 # Connection pooling for high-performance consumers
├── src/main/resources/
│   └── application.properties                   # Default application configuration
├── etc/kafka-rest/
│   └── er-kafka-rest.properties                 # External configuration 
├── target/
│   ├── classes/                                 # Compiled Java classes
│   └── kafka-offset-reader-*.jar               # Built application JAR
├── pom.xml                                      # Maven configuration with patched Kafka dependency
├── README.md                                    # Complete documentation
```
