package models

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Record struct {
	ID        int32  `json:"id"`
	Name      string `json:"name"`
	Address   string `json:"address"`
	Continent string `json:"continent"`
}

// ToCSV renders one RFC 4180 row (commas, quotes, and newlines in fields are escaped).
func (r *Record) ToCSV() string {
	var buf strings.Builder
	buf.Grow(48 + len(r.Name) + len(r.Address) + len(r.Continent))
	w := csv.NewWriter(&buf)
	if err := w.Write([]string{
		strconv.FormatInt(int64(r.ID), 10),
		r.Name,
		r.Address,
		r.Continent,
	}); err != nil {
		// csv.Writer only errors on write to underlying io.Writer; strings.Builder does not fail
		panic(err)
	}
	w.Flush()
	return strings.TrimSuffix(buf.String(), "\n")
}

// RecordFromFields parses four logical columns after CSV decoding.
func RecordFromFields(fields []string) (*Record, error) {
	if len(fields) != 4 {
		return nil, fmt.Errorf("invalid csv: want 4 fields, got %d", len(fields))
	}
	id, err := strconv.ParseInt(fields[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("parse id: %w", err)
	}
	return &Record{
		ID:        int32(id),
		Name:      fields[1],
		Address:   fields[2],
		Continent: fields[3],
	}, nil
}

// FromCSV decodes a single CSV record line (e.g. one Kafka message body).
func FromCSV(line string) (*Record, error) {
	r := csv.NewReader(strings.NewReader(line))
	r.FieldsPerRecord = 4
	r.LazyQuotes = true
	fields, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("csv read: %w", err)
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("invalid csv: trailing data after first record")
	}
	return RecordFromFields(fields)
}

// WriteCSVRow writes one record as a CSV line (including trailing newline) to w.
func WriteCSVRow(w *csv.Writer, r *Record) error {
	return w.Write([]string{
		strconv.FormatInt(int64(r.ID), 10),
		r.Name,
		r.Address,
		r.Continent,
	})
}

// NewChunkCSVWriter returns a csv.Writer configured for one-record-per-line chunk files.
func NewChunkCSVWriter(w io.Writer) *csv.Writer {
	cw := csv.NewWriter(w)
	return cw
}

// NewChunkCSVReader returns a csv.Reader for chunk / merge streams.
func NewChunkCSVReader(r io.Reader) *csv.Reader {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = 4
	cr.LazyQuotes = true
	return cr
}

// ReadRecord reads the next row from a chunk/merge csv.Reader. At end of input it returns nil, nil.
func ReadRecord(cr *csv.Reader) (*Record, error) {
	fields, err := cr.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}
	return RecordFromFields(fields)
}

// AppendKafkaCSVLine writes one CSV record into buf (no trailing newline, matching ToCSV).
func AppendKafkaCSVLine(buf *bytes.Buffer, r *Record) error {
	buf.Reset()
	w := csv.NewWriter(buf)
	if err := WriteCSVRow(w, r); err != nil {
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	for len(buf.Bytes()) > 0 && buf.Bytes()[len(buf.Bytes())-1] == '\n' {
		buf.Truncate(len(buf.Bytes()) - 1)
	}
	return nil
}
