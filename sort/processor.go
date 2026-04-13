package sort

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/NambariLikhitha/go-kafka-pipeline/models"
	"github.com/segmentio/kafka-go"
)

// PrintMemUsage actively dumps memory to stdout to prove tight footprint (under 2GB)
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB\tSys = %v MiB\tNumGC = %v\n", bToMb(m.Alloc), bToMb(m.Sys), m.NumGC)
}

func bToMb(b uint64) uint64 { return b / 1024 / 1024 }

type chunkCollector struct {
	mu                            sync.Mutex
	id, name, continent []string
	globalChunkCounter int
}

func (c *chunkCollector) add(idFile, nameFile, contFile string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.id = append(c.id, idFile)
	c.name = append(c.name, nameFile)
	c.continent = append(c.continent, contFile)
	c.globalChunkCounter++
	n := c.globalChunkCounter
	if n%1000 == 0 || n == 1 {
		runtime.GC()
		fmt.Printf("[Checkpoint Disk Flush] %d chunk triplets flushed... Mem Check: ", n)
		PrintMemUsage()
	}
}

func consumePartition(
	ctx context.Context,
	broker, topic string,
	partition int,
	expected int64,
	chunkSize int,
	collector *chunkCollector,
) error {
	if expected == 0 {
		return nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		Partition:      partition,
		StartOffset:    kafka.FirstOffset,
		MinBytes:       128 * 1024,
		MaxBytes:       10 << 20,
		MaxWait:        250 * time.Millisecond,
		CommitInterval: 0,
	})
	defer reader.Close()

	records := make([]*models.Record, 0, chunkSize)
	localSeq := 0

	for read := int64(0); read < expected; read++ {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("partition %d read: %w", partition, err)
		}

		rec, err := models.FromCSV(string(m.Value))
		if err != nil {
			return fmt.Errorf("partition %d parse: %w", partition, err)
		}
		records = append(records, rec)

		fullChunk := len(records) >= chunkSize
		lastInPart := read == expected-1
		if fullChunk || lastInPart {
			idF, nameF, contF, err := WriteSortedChunks(records, partition, localSeq, "data/chunks")
			if err != nil {
				return err
			}
			collector.add(idF, nameF, contF)
			localSeq++
			records = make([]*models.Record, 0, chunkSize)
		}
	}

	return nil
}

// Process reads a multi-partition source topic in parallel (one goroutine per partition),
// writes sorted chunk files, then merges globally to single-partition id/name/continent topics.
func Process(ctx context.Context, broker, sourceTopic string, totalRecords, chunkSize, sourcePartitions int) (time.Duration, time.Duration, error) {
	if sourcePartitions < 1 {
		sourcePartitions = 1
	}

	if err := os.MkdirAll("data/chunks", 0755); err != nil {
		return 0, 0, err
	}

	counts, err := PartitionLogLengths(ctx, broker, sourceTopic, sourcePartitions)
	if err != nil {
		return 0, 0, err
	}
	var sum int64
	for _, n := range counts {
		sum += n
	}
	if sum != int64(totalRecords) {
		return 0, 0, fmt.Errorf("partition message count %d != expected totalRecords %d (producer finished?)", sum, totalRecords)
	}

	fmt.Printf("Consumption started (%d partitions, %d-record chunks). Splitting to disk.\n", sourcePartitions, chunkSize)

	ingestStart := time.Now()
	var collector chunkCollector
	var wg sync.WaitGroup
	errCh := make(chan error, sourcePartitions)

	for p := 0; p < sourcePartitions; p++ {
		p := p
		exp := counts[p]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := consumePartition(ctx, broker, sourceTopic, p, exp, chunkSize, &collector); e != nil {
				errCh <- e
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			return 0, 0, e
		}
	}
	ingestDuration := time.Since(ingestStart)
	fmt.Printf("Ingestion phase (read source → chunk files): %v\n", ingestDuration)

	fmt.Println("Ingestion complete. Starting Sequential N-Way Merge to Kafka...")
	mergeStart := time.Now()

	// Sequential execution to keep RAM usage strictly low (below 2GB total cluster limit)
	if err := NWayMerge(ctx, collector.id, SortByID, broker, "id"); err != nil {
		return 0, 0, err
	}
	runtime.GC()

	if err := NWayMerge(ctx, collector.name, SortByName, broker, "name"); err != nil {
		return 0, 0, err
	}
	runtime.GC()

	if err := NWayMerge(ctx, collector.continent, SortByContinent, broker, "continent"); err != nil {
		return 0, 0, err
	}
	runtime.GC()

	mergeDuration := time.Since(mergeStart)
	fmt.Printf("Merge-to-Kafka phase (N-way merge → id/name/continent): %v\n", mergeDuration)
	return ingestDuration, mergeDuration, nil
}
