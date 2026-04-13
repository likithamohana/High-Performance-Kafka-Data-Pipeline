package sort

import (
	"container/heap"
	"testing"
	"github.com/NambariLikhitha/go-kafka-pipeline/models"
)

func TestRecordHeap(t *testing.T) {
	nodes := []*heapNode{
		{record: &models.Record{ID: 10, Name: "Z", Address: "C", Continent: "NA"}, streamIdx: 0},
		{record: &models.Record{ID: 1, Name: "A", Address: "B", Continent: "AS"}, streamIdx: 1},
		{record: &models.Record{ID: 5, Name: "M", Address: "A", Continent: "AF"}, streamIdx: 2},
	}

	// Test Min-Heap by ID
	hID := &recordHeap{sortType: SortByID, nodes: append([]*heapNode(nil), nodes...)}
	heap.Init(hID)
	root := heap.Pop(hID).(*heapNode)
	if root.record.ID != 1 {
		t.Errorf("Min-Heap by ID failed, got ID %d want 1", root.record.ID)
	}

	// Test Min-Heap by Name
	hName := &recordHeap{sortType: SortByName, nodes: append([]*heapNode(nil), nodes...)}
	heap.Init(hName)
	root = heap.Pop(hName).(*heapNode)
	if root.record.Name != "A" {
		t.Errorf("Min-Heap by Name failed, got %s want 'A'", root.record.Name)
	}

	// Test Min-Heap by Continent
	hCont := &recordHeap{sortType: SortByContinent, nodes: append([]*heapNode(nil), nodes...)}
	heap.Init(hCont)
	root = heap.Pop(hCont).(*heapNode)
	if root.record.Continent != "AF" {
		t.Errorf("Min-Heap by Continent failed, got %s want 'AF'", root.record.Continent)
	}
}
