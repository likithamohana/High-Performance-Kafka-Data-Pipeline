package source

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NambariLikhitha/go-kafka-pipeline/models"
	"github.com/segmentio/kafka-go"
)

const (
	letters  = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	alphanum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
)

var continents = []string{
	"North America", "Asia", "South America", "Europe", "Africa", "Australia",
}

// tuned for throughput: one writer per worker avoids internal lock contention on a shared kafka.Writer.
const (
	genBatchSize   = 20000
	genBatchBytes  = 16 << 20
	genBatchLinger = 50 * time.Millisecond
)

func randString(r *rand.Rand, n int, charset string) string {
	b := make([]byte, n)
	cl := len(charset)
	for i := range b {
		b[i] = charset[r.IntN(cl)]
	}
	return string(b)
}

func generateRecord(r *rand.Rand, id int64) *models.Record {
	return &models.Record{
		ID:        int32(id),
		Name:      randString(r, 10+r.IntN(6), letters),
		Address:   randString(r, 15+r.IntN(6), alphanum),
		Continent: continents[r.IntN(len(continents))],
	}
}

// GenerateRecord builds one record (uses a fresh PCG stream; fine for tests).
func GenerateRecord() *models.Record {
	return generateRecord(rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0)), 0)
}

func newGenWriter(broker, topic string) *kafka.Writer {
	// Uncompressed: reliable with embedded Kafka in Docker (Snappy has caused opaque broker errors).
	return &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		BatchSize:              genBatchSize,
		BatchBytes:             genBatchBytes,
		BatchTimeout:           genBatchLinger,
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
		AllowAutoTopicCreation: true,
	}
}

// GenerateAndPublish pushes records in batched writes; work is split across workers with no dropped remainder.
func GenerateAndPublish(ctx context.Context, broker, topic string, totalRecords int, concurrency int) error {
	if concurrency < 1 {
		concurrency = 1
	}

	base := totalRecords / concurrency
	rem := totalRecords % concurrency

	var wg sync.WaitGroup
	errChan := make(chan error, concurrency)
	var written atomic.Uint64

	doneProgress := make(chan struct{})
	var wgProgress sync.WaitGroup
	wgProgress.Add(1)
	go func() {
		defer wgProgress.Done()
		ticker := time.NewTicker(100 * time.Millisecond) // High-frequency poll to catch 5M marks
		defer ticker.Stop()

		const reportInterval = 5000000
		lastReportCount := uint64(0)
		startTime := time.Now()
		lastReportTime := startTime

		for {
			select {
			case <-ticker.C:
				n := written.Load()
				if n >= lastReportCount+reportInterval {
					now := time.Now()
					segmentDur := now.Sub(lastReportTime)
					totalDur := now.Sub(startTime)
					pct := 100.0 * float64(n) / float64(totalRecords)
					
					fmt.Printf("  >> [CHECKPOINT] Reached %d records (%.1f%%) | This 5M block: %v | Total Gen Time: %v\n", 
						n, pct, segmentDur.Round(time.Millisecond), totalDur.Round(time.Second))
					
					lastReportCount = (n / reportInterval) * reportInterval
					lastReportTime = now
				}
				if n >= uint64(totalRecords) {
					return
				}
			case <-doneProgress:
				// Final report
				n := written.Load()
				pct := 100.0 * float64(n) / float64(totalRecords)
				fmt.Printf("  generation complete: %d / %d records (%.2f%%) | Total Time: %v\n", 
					n, totalRecords, pct, time.Since(startTime).Round(time.Second))
				return
			}
		}
	}()

	fmt.Println("\n+----------------------------------------------------------------------+")
	fmt.Println("|                 KAFKA RECORD GENERATION SYSTEM STARTUP               |")
	fmt.Println("+----------------------------------------------------------------------+")
	fmt.Printf("|  Workers Spawning     : %d Parallel Goroutines\n", concurrency)
	fmt.Printf("|  Record Target        : %d Million Records\n", totalRecords/1000000)
	fmt.Printf("|  Concurrency Level    : %d (Optimized for 4-CPU cluster)\n", concurrency)
	fmt.Printf("|  Reporting Interval   : 5 Million Records\n")
	fmt.Println("+----------------------------------------------------------------------+")
	fmt.Println()

	fmt.Printf("Starting %d workers (~%d–%d records each)...\n", concurrency, base, base+1)

	// Use range-based global sequencing for speed and non-contention
	for i := 0; i < concurrency; i++ {
		count := base
		if i < rem {
			count++
		}
		if count == 0 {
			continue
		}

		// Calculate start ID for this worker to ensure global sequence within ranges
		startID := int64(0)
		for j := 0; j < i; j++ {
			c := base
			if j < rem {
				c++
			}
			startID += int64(c)
		}

		wg.Add(1)
		go func(wID int, currID int64, numToGen int) {
			defer wg.Done()
			w := newGenWriter(broker, topic)
			defer w.Close()

			// High-entropy seed for each worker
			seed := uint64(time.Now().UnixNano()) + uint64(wID)*1000000
			rng := rand.New(rand.NewPCG(seed, uint64(wID)))
			
			workerBatch := make([]kafka.Message, 0, genBatchSize)
			keyScratch := make([]byte, 0, 16)

			for j := 0; j < numToGen; j++ {
				rec := generateRecord(rng, currID+int64(j))
				keyScratch = strconv.AppendInt(keyScratch[:0], int64(rec.ID), 10)
				workerBatch = append(workerBatch, kafka.Message{
					Key:   append([]byte(nil), keyScratch...),
					Value: []byte(rec.ToCSV()),
				})

				if len(workerBatch) == genBatchSize {
					if err := w.WriteMessages(ctx, workerBatch...); err != nil {
						fmt.Printf("  [Worker %d] Error: %v\n", wID, err)
						errChan <- err
						return
					}
					written.Add(uint64(len(workerBatch)))
					workerBatch = workerBatch[:0]
				}
			}
			if len(workerBatch) > 0 {
				if err := w.WriteMessages(ctx, workerBatch...); err != nil {
					fmt.Printf("  [Worker %d] Error: %v\n", wID, err)
					errChan <- err
					return
				}
				written.Add(uint64(len(workerBatch)))
			}
		}(i, startID, count)
	}

	wg.Wait()
	close(doneProgress)
	wgProgress.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}
