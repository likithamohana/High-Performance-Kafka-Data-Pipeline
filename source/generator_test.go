package source

import (
	"math/rand/v2"
	"sync"
	"testing"
)

func TestGenerateRecord(t *testing.T) {
	record := GenerateRecord()

	if len(record.Name) < 10 || len(record.Name) > 15 {
		t.Errorf("Name length %d is outside bounds [10, 15]", len(record.Name))
	}

	if len(record.Address) < 15 || len(record.Address) > 20 {
		t.Errorf("Address length %d is outside bounds [15, 20]", len(record.Address))
	}

	validContinent := false
	for _, c := range continents {
		if c == record.Continent {
			validContinent = true
			break
		}
	}
	if !validContinent {
		t.Errorf("Continent %s is not valid", record.Continent)
	}
}

func TestGenerateRecord_Parallel(t *testing.T) {
	// Tests that the pseudo-random generator operates cleanly synchronously without crashing
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			GenerateRecord()
		}()
	}
	wg.Wait()
}

func TestRandStringBounds(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 1))
	s := randString(r, 12, letters)
	if len(s) != 12 {
		t.Errorf("Expected length 12, got %d", len(s))
	}
	for _, char := range s {
		valid := false
		for _, l := range letters {
			if char == l {
				valid = true
				break
			}
		}
		if !valid {
			t.Errorf("Invalid character generated: %c", char)
		}
	}
}
