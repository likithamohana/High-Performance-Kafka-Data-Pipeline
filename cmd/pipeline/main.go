package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/NambariLikhitha/go-kafka-pipeline/models"
	"github.com/NambariLikhitha/go-kafka-pipeline/sort"
	"github.com/NambariLikhitha/go-kafka-pipeline/source"
	"github.com/segmentio/kafka-go"
)

var (
	mode             = flag.String("mode", "full", "Mode: full | generate | process | verify")
	totalRecords     = flag.Int("total", 5000000, "Total records (generate/full); use 50000000 for full benchmark")
	chunkSize        = flag.Int("chunk", 1000000, "Chunk size for disk checkpointing")
	broker           = flag.String("broker", "localhost:9092", "Kafka broker address")
	concurrency      = flag.Int("concurrency", 4, "Generator worker goroutines (matches CPU for 4-core env)")
	sourcePartitions = flag.Int("source-partitions", 4, "Source topic partitions = parallel consumers (≤4 recommended for 4 CPU)")
	verifySamples    = flag.Int("verify-samples", 50000, "Max messages per output topic to check in verify mode")
)

// Topics: created in createTopics — source (sharded), id/name/continent (single partition, globally sorted).
func deleteTopics(b string, topics ...string) {
	conn, err := kafka.Dial("tcp", b)
	if err != nil {
		log.Printf("Failed dial for deletion: %v", err)
		return
	}
	defer conn.Close()

	if err := conn.DeleteTopics(topics...); err != nil {
		// Topic might not exist, that's fine
	}
	time.Sleep(2 * time.Second) // Wait for deletion to propagate
}

func createTopics(b string, topicPartitions map[string]int) {
	conn, err := kafka.Dial("tcp", b)
	if err != nil {
		log.Printf("Failed dial: %v", err)
		return
	}
	defer conn.Close()

	var topicConfigs []kafka.TopicConfig
	for topic, n := range topicPartitions {
		if n < 1 {
			n = 1
		}
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     n,
			ReplicationFactor: 1,
		})
	}
	if err := conn.CreateTopics(topicConfigs...); err != nil {
		log.Printf("CreateTopics: %v", err)
	}
	time.Sleep(1 * time.Second)
}

func printSampleRecords(b, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{b}, Topic: topic, Partition: 0, MinBytes: 10e3, MaxBytes: 10e6,
	})
	defer reader.Close()

	fmt.Printf("\n--- [Output Sample] First 10 records from globally sorted topic [%s] ---\n", topic)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			break
		}
		// Intercept the raw 4-column CSV and strip out Address just for printing
		rec, err := models.FromCSV(string(m.Value))
		if err == nil {
			fmt.Printf(" #%d: ID=%d | Name=%s | Continent=%s\n", i+1, rec.ID, rec.Name, rec.Continent)
		} else {
			// Fallback if parsing fails
			fmt.Println(string(m.Value))
		}
	}
}

func main() {

	flag.Parse()

	log.SetFlags(0)
	ctx := context.Background()
	start := time.Now()
	switch strings.ToLower(*mode) {
	case "full":
		log.Println("Running Full Pipeline...")

		// Ensure a clean state for "Exactly 50M" guarantee
		log.Println("Cleaning up topics for a fresh start...")
		deleteTopics(*broker, "source", "id", "name", "continent")

		createTopics(*broker, map[string]int{
			"source":    *sourcePartitions,
			"id":        1,
			"name":      1,
			"continent": 1,
		})

		genStart := time.Now()
		if err := source.GenerateAndPublish(ctx, *broker, "source", *totalRecords, *concurrency); err != nil {
			log.Fatalf("Generation failed: %v", err)
		}
		genDuration := time.Since(genStart)
		fmt.Printf("Generation phase: %v\n", genDuration)

		ingestDuration, mergeDuration, err := sort.Process(ctx, *broker, "source", *totalRecords, *chunkSize, *sourcePartitions)
		if err != nil {
			log.Fatalf("Processing failed: %v", err)
		}
		totalDuration := time.Since(start)
		sorterTotal := ingestDuration + mergeDuration

		fmt.Println("\n✅ Pipeline execution finished successfully.")
		fmt.Println("\n+----------------------------------------------------------------------+")
		fmt.Println("|            GOLANG + KAFKA PERFORMANCE MATRIX (WALL-CLOCK)            |")
		fmt.Println("+----------------------------------------------------------------------+")
		fmt.Println("|")
		fmt.Printf("|  [Phase 1] Data Generation (50M Mock Records)     : %v\n", genDuration.Round(time.Second))
		fmt.Printf("|  [Phase 2] PDQSort & Local Memory Chunk Spilling  : %v\n", ingestDuration.Round(time.Second))
		fmt.Printf("|  [Phase 3] N-Way Min-Heap Merge (3-Topic Output)  : %v\n", mergeDuration.Round(time.Second))
		fmt.Println("|")
		fmt.Printf("|  >> Total Sorter Compute Time (Phase 2 + Phase 3) : %v\n", sorterTotal.Round(time.Second))
		fmt.Printf("|  >> Absolute Pipeline Execution Time              : %v\n", totalDuration.Round(time.Second))
		fmt.Println("|")
		fmt.Println("+----------------------------------------------------------------------+")
		fmt.Println()
		fmt.Println("💾 App log backed up to: /app/data/pipeline.log")
		fmt.Println("   (Extract using: docker cp <container_name>:/app/data/pipeline.log .)")
		fmt.Println()

		printSampleRecords(*broker, "id")
		printSampleRecords(*broker, "name")
		printSampleRecords(*broker, "continent")

	case "generate":
		log.Println("Mode: generate → topic source only")
		log.Println("Cleaning up source topic for a fresh start...")
		deleteTopics(*broker, "source")

		createTopics(*broker, map[string]int{
			"source": *sourcePartitions,
		})

		if err := source.GenerateAndPublish(ctx, *broker, "source", *totalRecords, *concurrency); err != nil {
			log.Fatalf("Generation failed: %v", err)
		}
		fmt.Printf("Generation done in %v\n", time.Since(start))

	case "process":
		log.Println("Mode: process → read source, sort/merge → id, name, continent")
		ingestDuration, mergeDuration, err := sort.Process(ctx, *broker, "source", *totalRecords, *chunkSize, *sourcePartitions)
		if err != nil {
			log.Fatalf("Processing failed: %v", err)
		}

		totalDuration := time.Since(start)
		sorterTotal := ingestDuration + mergeDuration

		fmt.Println("\n✅ Pipeline execution finished successfully.")
		fmt.Println("\n+----------------------------------------------------------------------+")
		fmt.Println("|         PROCESSOR ENGINE PERFORMANCE MATRIX (WALL-CLOCK)             |")
		fmt.Println("+----------------------------------------------------------------------+")
		fmt.Println("|")
		fmt.Printf("|  [Phase 1] PDQSort & Local Memory Chunk Spilling  : %v\n", ingestDuration.Round(time.Second))
		fmt.Printf("|  [Phase 2] N-Way Min-Heap Merge (3-Topic Output)  : %v\n", mergeDuration.Round(time.Second))
		fmt.Println("|")
		fmt.Printf("|  >> Total Sorter Compute Time                     : %v\n", sorterTotal.Round(time.Second))
		fmt.Printf("|  >> Absolute Pipeline Execution Time              : %v\n", totalDuration.Round(time.Second))
		fmt.Println("|")
		fmt.Println("+----------------------------------------------------------------------+")
		fmt.Println()
		fmt.Println("💾 App log backed up to: /app/data/pipeline.log")
		fmt.Println("   (Extract using: docker cp <container_name>:/app/data/pipeline.log .)")
		fmt.Println()

	case "verify":
		log.Printf("Mode: verify — checking sort order on id, name, continent (up to %d msgs each)\n", *verifySamples)
		if err := sort.VerifySortedOutput(ctx, *broker, *verifySamples); err != nil {
			log.Fatalf("Verify failed: %v", err)
		}
		fmt.Println("All verification checks passed.")

	default:
		log.Fatalf("Unknown -mode=%q (use full, generate, process, verify)", *mode)
	}
}
