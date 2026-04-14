#!/bin/bash
set -e

# Enough heap for broker under load; still fits typical 2G container with the Go binary.
export KAFKA_HEAP_OPTS="-Xmx384M -Xms384M"
export GOMAXPROCS=3
export GOGC=50

echo "Booting Zookeeper..."
/opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
sleep 2

echo "Tuning Kafka broker limits..."
cat /app/kafka-extra.properties >> /opt/kafka/config/server.properties

echo "Booting Kafka..."
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties

echo "Waiting for Kafka to listen on 9092..."
until nc -z localhost 9092; do
  sleep 1
done

# Quick metadata warming
sleep 5

echo "Starting the Kafka Sort Pipeline application..."
# Default 50M records for the full scale execution
TOTAL_RECORDS="${TOTAL_RECORDS:-50000000}"
echo "TOTAL_RECORDS=${TOTAL_RECORDS} (override with -e TOTAL_RECORDS=...; e.g. 50000000 for full load)"
time /app/kafka-sort -mode=full -broker=localhost:9092 -chunk=1000000 -source-partitions=4 -concurrency=4 -total="${TOTAL_RECORDS}" 2>&1 | tee /app/data/pipeline.log

echo "-----------------------------------"
echo "  Pipeline execution is complete!  "
echo "-----------------------------------"
echo "Container is idling. You can now use 'docker exec' to check messages."

# Keep container alive to allow testing matching the user's instructions
tail -f /dev/null
