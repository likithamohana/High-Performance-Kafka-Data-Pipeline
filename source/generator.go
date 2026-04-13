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
	genBatchSize   = 10000
	genBatchBytes  = 12 << 20
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

func generateRecord(r *rand.Rand) *models.Record {
	return &models.Record{
		ID:        r.Int32(),
		Name:      randString(r, 10+r.IntN(6), letters),
		Address:   randString(r, 15+r.IntN(6), alphanum),
		Continent: continents[r.IntN(len(continents))],
	}
}

// GenerateRecord builds one record (uses a fresh PCG stream; fine for tests).
func GenerateRecord() *models.Record {
	return generateRecord(rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0)))
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
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n := written.Load()
				if n >= uint64(totalRecords) {
					return
				}
				pct := 100.0 * float64(n) / float64(totalRecords)
				fmt.Printf("  generation progress: %d / %d records (%.2f%%)\n", n, totalRecords, pct)
			case <-doneProgress:
				return
			}
		}
	}()

	fmt.Printf("Starting %d workers (~%d–%d records each)...\n", concurrency, base, base+1)

	for i := 0; i < concurrency; i++ {
		n := base
		if i < rem {
			n++
		}
		if n == 0 {
			continue
		}

		wg.Add(1)
		go func(workerID, count int) {
			defer wg.Done()
			w := newGenWriter(broker, topic)
			defer w.Close()

			rng := rand.New(rand.NewPCG(uint64(workerID), uint64(time.Now().UnixNano())))
			workerBatch := make([]kafka.Message, 0, genBatchSize)
			keyScratch := make([]byte, 0, 16)

			for j := 0; j < count; j++ {
				rec := generateRecord(rng)
				keyScratch = strconv.AppendInt(keyScratch[:0], int64(rec.ID), 10)
				workerBatch = append(workerBatch, kafka.Message{
					Key:   append([]byte(nil), keyScratch...),
					Value: []byte(rec.ToCSV()),
				})

				if len(workerBatch) == genBatchSize {
					if err := w.WriteMessages(ctx, workerBatch...); err != nil {
						errChan <- err
						return
					}
					written.Add(uint64(len(workerBatch)))
					workerBatch = workerBatch[:0]
				}
			}
			if len(workerBatch) > 0 {
				if err := w.WriteMessages(ctx, workerBatch...); err != nil {
					errChan <- err
					return
				}
				written.Add(uint64(len(workerBatch)))
			}
		}(i, n)
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
