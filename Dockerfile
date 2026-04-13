FROM golang:alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o bin/kafka-sort cmd/pipeline/main.go

FROM eclipse-temurin:11-jre-alpine
WORKDIR /app

# Install bash required by Kafka scripts
RUN apk add --no-cache bash wget tar netcat-openbsd

# Download and extract Kafka locally
ENV KAFKA_VERSION=3.6.1
ENV SCALA_VERSION=2.13
RUN wget -q "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -O /tmp/kafka.tgz && \
    mkdir -p /opt/kafka && \
    tar -xzf /tmp/kafka.tgz -C /opt/kafka --strip-components=1 && \
    rm /tmp/kafka.tgz

COPY --from=builder /app/bin/kafka-sort /app/kafka-sort
RUN mkdir -p /app/data/chunks

COPY scripts/entrypoint.sh /app/entrypoint.sh
COPY scripts/verify.sh /app/verify.sh
COPY docker/kafka-extra.properties /app/kafka-extra.properties
RUN chmod +x /app/entrypoint.sh /app/verify.sh

CMD ["/app/entrypoint.sh"]
