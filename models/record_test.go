package models

import (
	"bytes"
	"strings"
	"testing"
)

func TestFromCSV(t *testing.T) {
	validCSV := "42,John Doe,123 Main St,North America"
	rec, err := FromCSV(validCSV)
	if err != nil {
		t.Fatalf("unexpected error parsing valid CSV: %v", err)
	}
	if rec.ID != 42 || rec.Name != "John Doe" || rec.Address != "123 Main St" || rec.Continent != "North America" {
		t.Errorf("FromCSV got bad data: %+v", rec)
	}
}

func TestCSVRoundtripCommaAndQuote(t *testing.T) {
	rec := &Record{
		ID:        42,
		Name:      `O'Brien, "Pat"`,
		Address:   `123 Main St, Apt 4`,
		Continent: "Europe",
	}
	encoded := rec.ToCSV()
	decoded, err := FromCSV(encoded)
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}
	if decoded.ID != rec.ID || decoded.Name != rec.Name || decoded.Address != rec.Address || decoded.Continent != rec.Continent {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", decoded, rec)
	}
	if !strings.Contains(encoded, ",") {
		t.Fatal("encoded CSV should contain field separators")
	}
}

func TestFromCSVInvalid(t *testing.T) {
	invalidCases := []string{
		"1,2,3", // not enough fields
		"abc,John,123 Main St,North America", // bad ID type
		"", // empty string
	}

	for _, tc := range invalidCases {
		_, err := FromCSV(tc)
		if err == nil {
			t.Errorf("expected error for invalid CSV: %s", tc)
		}
	}
}

func TestAppendKafkaCSVLine(t *testing.T) {
	buf := new(bytes.Buffer)
	rec := &Record{ID: 10, Name: "A", Address: "B", Continent: "Asia"}
	
	err := AppendKafkaCSVLine(buf, rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	
	str := buf.String()
	if str != "10,A,B,Asia" && str != "10,A,B,Asia\n" {
		t.Errorf("Appended invalid line: %q", str)
	}
}

func TestChunkCSVReaderWriter(t *testing.T) {
	buf := new(bytes.Buffer)
	cw := NewChunkCSVWriter(buf)
	
	rec := &Record{ID: 1, Name: "Test", Address: "Addr", Continent: "Asia"}
	err := WriteCSVRow(cw, rec)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	cw.Flush()
	
	cr := NewChunkCSVReader(buf)
	readRec, err := ReadRecord(cr)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	
	if readRec.ID != rec.ID {
		t.Errorf("expected read record to match written, got %v", readRec.ID)
	}
}
