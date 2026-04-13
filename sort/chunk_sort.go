package sort

import (
	"bufio"
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/NambariLikhitha/go-kafka-pipeline/models"
)

type SortType string

const (
	SortByID        SortType = "id"
	SortByName      SortType = "name"
	SortByContinent SortType = "continent"
)

func sortSliceInPlace(slice []*models.Record, sortType SortType) {
	switch sortType {
	case SortByID:
		slices.SortFunc(slice, func(a, b *models.Record) int {
			return cmp.Compare(a.ID, b.ID)
		})
	case SortByName:
		slices.SortFunc(slice, func(a, b *models.Record) int {
			return cmp.Compare(a.Name, b.Name)
		})
	case SortByContinent:
		slices.SortFunc(slice, func(a, b *models.Record) int {
			return cmp.Compare(a.Continent, b.Continent)
		})
	}
}

func writeSortedChunkToFile(sorted []*models.Record, partition, seq int, sortType SortType, tmpDir string) (string, error) {
	fileName := filepath.Join(tmpDir, fmt.Sprintf("chunk_p%02d_s%05d_%s.csv", partition, seq, sortType))
	f, err := os.Create(fileName)
	if err != nil {
		return "", err
	}
	defer f.Close()

	bw := bufio.NewWriterSize(f, 256*1024)
	cw := models.NewChunkCSVWriter(bw)
	for _, r := range sorted {
		if err := models.WriteCSVRow(cw, r); err != nil {
			return "", err
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return "", err
	}
	if err := bw.Flush(); err != nil {
		return "", err
	}
	return fileName, nil
}

// SortAndWriteChunk copies records, sorts, and writes one chunk file (tests / tooling).
func SortAndWriteChunk(records []*models.Record, partition, seq int, sortType SortType, tmpDir string) (string, error) {
	cpy := make([]*models.Record, len(records))
	copy(cpy, records)
	sortSliceInPlace(cpy, sortType)
	return writeSortedChunkToFile(cpy, partition, seq, sortType, tmpDir)
}

// WriteSortedChunks sorts the same logical chunk three ways using one working buffer (~1× chunk memory).
func WriteSortedChunks(records []*models.Record, partition, seq int, tmpDir string) (idFile, nameFile, contFile string, err error) {
	work := make([]*models.Record, len(records))
	copy(work, records)
	sortSliceInPlace(work, SortByID)
	idFile, err = writeSortedChunkToFile(work, partition, seq, SortByID, tmpDir)
	if err != nil {
		return "", "", "", err
	}

	copy(work, records)
	sortSliceInPlace(work, SortByName)
	nameFile, err = writeSortedChunkToFile(work, partition, seq, SortByName, tmpDir)
	if err != nil {
		return "", "", "", err
	}

	copy(work, records)
	sortSliceInPlace(work, SortByContinent)
	contFile, err = writeSortedChunkToFile(work, partition, seq, SortByContinent, tmpDir)
	if err != nil {
		return "", "", "", err
	}

	return idFile, nameFile, contFile, nil
}
