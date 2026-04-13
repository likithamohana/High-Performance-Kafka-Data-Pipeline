package sort

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/NambariLikhitha/go-kafka-pipeline/models"
	"github.com/segmentio/kafka-go"
)

var mergeLineBufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func recordToKafkaValue(rec *models.Record) ([]byte, error) {
	buf := mergeLineBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	if err := models.AppendKafkaCSVLine(buf, rec); err != nil {
		mergeLineBufPool.Put(buf)
		return nil, err
	}
	out := append([]byte(nil), buf.Bytes()...)
	mergeLineBufPool.Put(buf)
	return out, nil
}

type heapNode struct {
	record    *models.Record
	streamIdx int
}

type recordHeap struct {
	nodes    []*heapNode
	sortType SortType
}

func (h recordHeap) Len() int { return len(h.nodes) }
func (h recordHeap) Less(i, j int) bool {
	a := h.nodes[i].record
	b := h.nodes[j].record
	switch h.sortType {
	case SortByID:
		return a.ID < b.ID
	case SortByName:
		return a.Name < b.Name
	case SortByContinent:
		return a.Continent < b.Continent
	}
	return false
}
func (h recordHeap) Swap(i, j int)       { h.nodes[i], h.nodes[j] = h.nodes[j], h.nodes[i] }
func (h *recordHeap) Push(x interface{}) { h.nodes = append(h.nodes, x.(*heapNode)) }
func (h *recordHeap) Pop() interface{} {
	old := h.nodes
	n := len(old)
	item := old[n-1]
	h.nodes = old[0 : n-1]
	return item
}

type mergeStream struct {
	f  *os.File
	cr *csv.Reader
}

const mergeBatchSize = 10000

func NWayMerge(ctx context.Context, chunkFiles []string, sortType SortType, kafkaBroker, targetTopic string) error {
	var streams []mergeStream

	defer func() {
		for i := range streams {
			if streams[i].f != nil {
				streams[i].f.Close()
			}
		}
		for _, fn := range chunkFiles {
			os.Remove(fn)
		}
	}()

	h := &recordHeap{sortType: sortType, nodes: make([]*heapNode, 0, len(chunkFiles))}

	for i, fn := range chunkFiles {
		f, err := os.Open(fn)
		if err != nil {
			return err
		}
		cr := models.NewChunkCSVReader(f)
		streams = append(streams, mergeStream{f: f, cr: cr})

		rec, err := models.ReadRecord(cr)
		if err != nil {
			return fmt.Errorf("file %s: %w", fn, err)
		}
		if rec != nil {
			h.nodes = append(h.nodes, &heapNode{record: rec, streamIdx: i})
		}
	}

	heap.Init(h)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBroker),
		Topic:                  targetTopic,
		BatchSize:              mergeBatchSize,
		BatchBytes:             12 << 20,
		BatchTimeout:           50 * time.Millisecond,
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	batch := make([]kafka.Message, 0, mergeBatchSize)
	mergedCount := 0

	for h.Len() > 0 {
		minNode := heap.Pop(h).(*heapNode)
		val, err := recordToKafkaValue(minNode.record)
		if err != nil {
			return err
		}
		batch = append(batch, kafka.Message{Value: val})

		if len(batch) >= mergeBatchSize {
			if err := writer.WriteMessages(ctx, batch...); err != nil {
				return err
			}
			batch = batch[:0]
			mergedCount += mergeBatchSize
			if mergedCount%5000000 == 0 {
				fmt.Printf("[%s Merge] merged %d records. Memory: ", sortType, mergedCount)
				PrintMemUsage()
				runtime.GC()
			}
		}

		nextRec, err := models.ReadRecord(streams[minNode.streamIdx].cr)
		if err != nil {
			return err
		}
		if nextRec != nil {
			heap.Push(h, &heapNode{record: nextRec, streamIdx: minNode.streamIdx})
		}
	}

	if len(batch) > 0 {
		return writer.WriteMessages(ctx, batch...)
	}
	return nil
}
