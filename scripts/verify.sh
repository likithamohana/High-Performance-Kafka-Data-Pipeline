#!/bin/sh
# Run inside the container (or from host with -broker host.docker.internal:9092 if exposed).
# Validates global sort order on id, name, continent topics; each message is full CSV: id,name,address,continent

set -e
BROKER="${KAFKA_BROKER:-localhost:9092}"
SAMPLES="${VERIFY_SAMPLES:-50000}"

exec /app/kafka-sort -mode=verify -broker="$BROKER" -verify-samples="$SAMPLES"
