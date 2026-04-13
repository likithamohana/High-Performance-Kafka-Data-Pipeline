package sort

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// PartitionLogLengths returns the number of readable messages in each partition
// (high offset − low offset) after the producer has finished writing.
func PartitionLogLengths(ctx context.Context, broker, topic string, partitions int) ([]int64, error) {
	counts := make([]int64, partitions)
	for p := 0; p < partitions; p++ {
		conn, err := kafka.DialLeader(ctx, "tcp", broker, topic, p)
		if err != nil {
			return nil, fmt.Errorf("dial partition %d: %w", p, err)
		}
		first, last, err := conn.ReadOffsets()
		_ = conn.Close()
		if err != nil {
			return nil, fmt.Errorf("offsets partition %d: %w", p, err)
		}
		n := last - first
		if n < 0 {
			n = 0
		}
		counts[p] = n
	}
	return counts, nil
}
