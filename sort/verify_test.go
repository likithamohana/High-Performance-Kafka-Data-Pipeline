package sort

import (
	"testing"
	"github.com/NambariLikhitha/go-kafka-pipeline/models"
)

func TestCompareBySortType(t *testing.T) {
	a := &models.Record{ID: 1, Name: "Alpha", Continent: "Africa", Address: "A Street"}
	b := &models.Record{ID: 2, Name: "Beta", Continent: "Asia", Address: "B Street"}

	if compareBySortType(a, b, SortByID) >= 0 {
		t.Error("a.ID < b.ID")
	}
	if compareBySortType(a, b, SortByName) >= 0 {
		t.Error("a.Name < b.Name")
	}
	if compareBySortType(a, b, SortByContinent) >= 0 {
		t.Error("a.Continent < b.Continent")
	}
	
	if compareBySortType(b, a, SortByID) <= 0 {
		t.Error("b.ID > a.ID")
	}
	if compareBySortType(a, a, SortByID) != 0 {
		t.Error("a.ID == a.ID")
	}
}
