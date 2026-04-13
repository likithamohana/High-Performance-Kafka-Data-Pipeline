package sort

import (
	"cmp"
	"context"
	"fmt"

	"github.com/NambariLikhitha/go-kafka-pipeline/models"
	"github.com/segmentio/kafka-go"
)

// VerifySortedOutput reads up to maxInspect messages from each output topic (partition 0)
// and checks monotonic order for the topic's sort key. Each row must parse as 3 CSV fields
// (id, name, continent).
func VerifySortedOutput(ctx context.Context, broker string, maxInspect int) error {
	if maxInspect < 2 {
		maxInspect = 2
	}
	checks := []struct {
		topic string
		st    SortType
	}{
		{"id", SortByID},
		{"name", SortByName},
		{"continent", SortByContinent},
	}
	for _, c := range checks {
		if err := verifyTopicOrder(ctx, broker, c.topic, c.st, maxInspect); err != nil {
			return err
		}
	}
	return nil
}

func compareBySortType(a, b *models.Record, st SortType) int {
	switch st {
	case SortByID:
		return cmp.Compare(a.ID, b.ID)
	case SortByName:
		return cmp.Compare(a.Name, b.Name)
	case SortByContinent:
		return cmp.Compare(a.Continent, b.Continent)
	default:
		return 0
	}
}

func verifyTopicOrder(ctx context.Context, broker, topic string, st SortType, maxN int) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		Partition:   0,
		StartOffset: kafka.FirstOffset,
	})
	defer r.Close()

	var prev *models.Record
	for i := 0; i < maxN; i++ {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if i == 0 {
				return fmt.Errorf("topic %q: read: %w", topic, err)
			}
			fmt.Printf("[verify] topic %q: stream ended after %d messages (OK so far)\n", topic, i)
			return nil
		}
		rec, err := models.FromCSV(string(m.Value))
		if err != nil {
			return fmt.Errorf("topic %q message %d: %w", topic, i, err)
		}
		if prev != nil {
			if compareBySortType(prev, rec, st) > 0 {
				return fmt.Errorf("topic %q: sort order broken at index %d (by %s): prev id=%d name=%q continent=%q | cur id=%d name=%q continent=%q",
					topic, i, st, prev.ID, prev.Name, prev.Continent, rec.ID, rec.Name, rec.Continent)
			}
		}
		prev = rec
	}
	fmt.Printf("[verify] topic %q: OK — first %d messages sorted by %s\n", topic, maxN, st)
	return nil
}
