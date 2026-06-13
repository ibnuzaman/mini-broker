package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "log-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestOpenLog_CreatesFile(t *testing.T) {
	dir := tempDir(t)
	l, err := OpenLog(dir, "test-topic")
	if err != nil {
		t.Fatalf("OpenLog failed: %v", err)
	}
	defer l.Close()

	path := filepath.Join(dir, "test-topic.log")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("log file was not created at %s", path)
	}
}

func TestOpenLog_CreatesDataDirIfNotExists(t *testing.T) {
	dir := tempDir(t)
	nested := filepath.Join(dir, "a", "b", "c")

	l, err := OpenLog(nested, "topic")
	if err != nil {
		t.Fatalf("OpenLog failed: %v", err)
	}
	defer l.Close()

	if _, err := os.Stat(nested); os.IsNotExist(err) {
		t.Errorf("data dir was not created: %s", nested)
	}
}

func TestOpenLog_StartsAtOffsetZeroForNewFile(t *testing.T) {
	dir := tempDir(t)
	l, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("OpenLog failed: %v", err)
	}
	defer l.Close()

	if l.nextOffset != 0 {
		t.Errorf("expected nextOffset 0, got %d", l.nextOffset)
	}
}

func TestAppend_ReturnsSequentialOffsets(t *testing.T) {
	dir := tempDir(t)
	l, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("OpenLog failed: %v", err)
	}
	defer l.Close()

	for i := int64(0); i < 5; i++ {
		offset, err := l.Append([]byte("msg"))
		if err != nil {
			t.Fatalf("Append failed at %d: %v", i, err)
		}
		if offset != i {
			t.Errorf("expected offset %d, got %d", i, offset)
		}
	}
}

func TestAppend_PersistsPayload(t *testing.T) {
	dir := tempDir(t)
	l, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("OpenLog failed: %v", err)
	}
	defer l.Close()

	msg := []byte("hello broker")
	if _, err := l.Append(msg); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	records, err := l.ReadFrom(0, 10)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if string(records[0].Payload) != string(msg) {
		t.Errorf("expected payload %q, got %q", msg, records[0].Payload)
	}
}

func TestReadFrom_ReturnsCorrectOffsets(t *testing.T) {
	dir := tempDir(t)
	l, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("OpenLog failed: %v", err)
	}
	defer l.Close()

	payloads := []string{"a", "b", "c", "d", "e"}
	for _, p := range payloads {
		if _, err := l.Append([]byte(p)); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	records, err := l.ReadFrom(2, 10)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records from offset 2, got %d", len(records))
	}
	for i, r := range records {
		expectedOffset := int64(i + 2)
		if r.Offset != expectedOffset {
			t.Errorf("record %d: expected offset %d, got %d", i, expectedOffset, r.Offset)
		}
		expectedPayload := payloads[i+2]
		if string(r.Payload) != expectedPayload {
			t.Errorf("record %d: expected payload %q, got %q", i, expectedPayload, r.Payload)
		}
	}
}

func TestReadFrom_RespectsMaxCount(t *testing.T) {
	dir := tempDir(t)
	l, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("OpenLog failed: %v", err)
	}
	defer l.Close()

	for i := 0; i < 10; i++ {
		l.Append([]byte("msg"))
	}

	records, err := l.ReadFrom(0, 3)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if len(records) != 3 {
		t.Errorf("expected 3 records (maxCount=3), got %d", len(records))
	}
}

func TestReadFrom_BeyondOffsetReturnsNil(t *testing.T) {
	dir := tempDir(t)
	l, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("OpenLog failed: %v", err)
	}
	defer l.Close()

	l.Append([]byte("only one"))

	records, err := l.ReadFrom(99, 10)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if records != nil {
		t.Errorf("expected nil for out-of-range offset, got %v", records)
	}
}

func TestReadFrom_TimestampIsPopulated(t *testing.T) {
	dir := tempDir(t)
	l, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("OpenLog failed: %v", err)
	}
	defer l.Close()

	l.Append([]byte("ts-check"))

	records, err := l.ReadFrom(0, 1)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if records[0].Timestamp <= 0 {
		t.Errorf("expected positive timestamp, got %d", records[0].Timestamp)
	}
}

func TestOpenLog_RecoverOffsetAfterReopen(t *testing.T) {
	dir := tempDir(t)

	l, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("first OpenLog failed: %v", err)
	}
	for i := 0; i < 5; i++ {
		l.Append([]byte("msg"))
	}
	l.Close()

	l2, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("second OpenLog failed: %v", err)
	}
	defer l2.Close()

	if l2.nextOffset != 5 {
		t.Errorf("expected nextOffset 5 after reopen, got %d", l2.nextOffset)
	}
}

func TestOpenLog_ContinuesAppendAfterReopen(t *testing.T) {
	dir := tempDir(t)

	l, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("first OpenLog failed: %v", err)
	}
	l.Append([]byte("first"))
	l.Close()

	l2, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("second OpenLog failed: %v", err)
	}
	defer l2.Close()

	offset, err := l2.Append([]byte("second"))
	if err != nil {
		t.Fatalf("Append after reopen failed: %v", err)
	}
	if offset != 1 {
		t.Errorf("expected offset 1 after reopen append, got %d", offset)
	}

	records, err := l2.ReadFrom(0, 10)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records after reopen, got %d", len(records))
	}
	if string(records[0].Payload) != "first" {
		t.Errorf("expected 'first', got %q", records[0].Payload)
	}
	if string(records[1].Payload) != "second" {
		t.Errorf("expected 'second', got %q", records[1].Payload)
	}
}

func TestAppend_EmptyPayload(t *testing.T) {
	dir := tempDir(t)
	l, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("OpenLog failed: %v", err)
	}
	defer l.Close()

	offset, err := l.Append([]byte{})
	if err != nil {
		t.Fatalf("Append empty payload failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("expected offset 0, got %d", offset)
	}

	records, err := l.ReadFrom(0, 1)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if len(records[0].Payload) != 0 {
		t.Errorf("expected empty payload, got %q", records[0].Payload)
	}
}

func TestClose_FlushesData(t *testing.T) {
	dir := tempDir(t)
	l, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("OpenLog failed: %v", err)
	}

	l.Append([]byte("flush-me"))
	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Buka ulang dan pastikan data ada
	l2, err := OpenLog(dir, "topic")
	if err != nil {
		t.Fatalf("second OpenLog failed: %v", err)
	}
	defer l2.Close()

	if l2.nextOffset != 1 {
		t.Errorf("expected 1 record after close+reopen, got nextOffset=%d", l2.nextOffset)
	}
}
