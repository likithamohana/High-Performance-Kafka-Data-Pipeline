package sort

import (
	"os"
	"testing"

	"github.com/NambariLikhitha/go-kafka-pipeline/models"
)

func TestSortSliceInPlace(t *testing.T) {
	records := []*models.Record{
		{ID: 5, Name: "Zeta", Address: "c", Continent: "Asia"},
		{ID: 1, Name: "Alpha", Address: "b", Continent: "Europe"},
		{ID: 3, Name: "Beta", Address: "a", Continent: "Africa"},
	}

	// Test ID Sort
	sortSliceInPlace(records, SortByID)
	if records[0].ID != 1 || records[2].ID != 5 {
		t.Errorf("SortByID failed: index 0 is %d", records[0].ID)
	}

	// Test Name Sort
	sortSliceInPlace(records, SortByName)
	if records[0].Name != "Alpha" || records[2].Name != "Zeta" {
		t.Errorf("SortByName failed: index 0 is %s", records[0].Name)
	}

	// Test Continent Sort
	sortSliceInPlace(records, SortByContinent)
	if records[0].Continent != "Africa" || records[2].Continent != "Europe" {
		t.Errorf("SortByContinent failed: index 0 is %s", records[0].Continent)
	}
}

func TestChunkSorting(t *testing.T) {
	records := []*models.Record{
		{ID: 5, Name: "Zeta", Address: "a", Continent: "Asia"},
		{ID: 1, Name: "Alpha", Address: "b", Continent: "Europe"},
		{ID: 3, Name: "Beta", Address: "c", Continent: "Africa"},
	}

	os.MkdirAll("data/chunks", 0755)

	idFile, err := SortAndWriteChunk(records, 0, 0, SortByID, "data/chunks")
	if err != nil {
		t.Fatalf("Failed to sort and write chunk: %v", err)
	}

	defer os.RemoveAll("data")

	if _, err := os.Stat(idFile); os.IsNotExist(err) {
		t.Errorf("Chunk file %s was not created", idFile)
	}
}

func TestWriteSortedChunksFull(t *testing.T) {
	records := []*models.Record{
		{ID: 5, Name: "Zeta", Address: "a", Continent: "Asia"},
		{ID: 1, Name: "Alpha", Address: "b", Continent: "Europe"},
	}

	os.MkdirAll("data/chunks", 0755)
	defer os.RemoveAll("data")

	f1, f2, f3, err := WriteSortedChunks(records, 1, 1, "data/chunks")
	if err != nil {
		t.Fatalf("Failed multi-write chunks: %v", err)
	}
	
	if f1 == "" || f2 == "" || f3 == "" {
		t.Errorf("Missing output chunk file names")
	}
}
