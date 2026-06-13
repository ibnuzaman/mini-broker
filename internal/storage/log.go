package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	recordHeaderSize = 12 // 8 timestamp + 4 (payload length)
)

// Log untuk satu topik-partisi
type Log struct {
	mu         sync.Mutex
	file       *os.File
	writer     *bufio.Writer
	nextOffset int64
	dataDir    string
	topic      string
}

// Membuka atau membuat logfile
func OpenLog(dataDir, topic string) (*Log, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("storage mkdir %s: %w", dataDir, err)
	}

	path := filepath.Join(dataDir, topic+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("storage open log %s: %w", path, err)
	}

	l := &Log{
		file:    f,
		writer:  bufio.NewWriterSize(f, 64*1024), // buffer 64KB
		dataDir: dataDir,
		topic:   topic,
	}

	// Hitung offset berikutnya record yang sudah ada
	if err := l.countExistingRecords(); err != nil {
		f.Close()
		return nil, fmt.Errorf("storage count existing records: %w", err)
	}

	return l, nil
}

// Append menyimpan message ke log dan mengembalikan offsetnya
func (l *Log) Append(payload []byte) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	offset := l.nextOffset
	ts := time.Now().UnixNano()

	// tulis header: timestamp (8 byte) + payload length (4 byte)
	header := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint64(header[0:8], uint64(ts))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(payload)))

	if _, err := l.writer.Write(header); err != nil {
		return 0, fmt.Errorf("storage: write header: %w", err)
	}
	if _, err := l.writer.Write(payload); err != nil {
		return 0, fmt.Errorf("storage: write payload: %w", err)
	}

	// flush biar langsung ke disk (penting buat durability)
	if err := l.writer.Flush(); err != nil {
		return 0, fmt.Errorf("storage: flush: %w", err)
	}

	l.nextOffset++
	return offset, nil
}

// ReadFrom membaca messages mulai dari offset tertentu, maksimal maxCount.
// Thread-safe (concurrent reads diperbolehkan).
func (l *Log) ReadFrom(startOffset int64, maxCount int) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if startOffset >= l.nextOffset {
		return nil, nil // offset di luar range
	}

	f, err := os.Open(l.file.Name())
	if err != nil {
		return nil, fmt.Errorf("storage: open log for read: %w", err)
	}

	defer f.Close()

	reader := bufio.NewReader(f)
	var results []Record
	var currentOffset int64 = 0

	for {
		if len(results) >= maxCount {
			break
		}

		// Baca header
		header := make([]byte, recordHeaderSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("storage: read header at offset %d: %w", currentOffset, err)
		}

		ts := int64(binary.BigEndian.Uint64(header[0:8]))
		payloadLen := binary.BigEndian.Uint32(header[8:12])

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, fmt.Errorf("storage: read payload at offset %d: %w", currentOffset, err)
		}

		// skip sampai offset yang diinginkan
		if currentOffset >= startOffset {
			results = append(results, Record{
				Offset:    currentOffset,
				Timestamp: ts,
				Payload:   payload,
			})
		}

		currentOffset++
	}

	return results, nil
}

// countExistingOffsets scan file dari awal buat tau udah ada berapa record.
func (l *Log) countExistingRecords() error {
	l.file.Seek(0, io.SeekStart)
	reader := bufio.NewReader(l.file)
	var count int64 = 0

	for {
		header := make([]byte, recordHeaderSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("storage: scan existing: %w", err)
		}
		payloadLen := binary.BigEndian.Uint32(header[8:12])
		if _, err := reader.Discard(int(payloadLen)); err != nil {
			break
		}
		count++
	}

	l.nextOffset = count
	return nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.writer.Flush(); err != nil {
		return fmt.Errorf("storage: flush on close: %w", err)
	}

	return l.file.Close()
}

// Record adalah satu entry yang dibaca dari log.
type Record struct {
	Offset    int64
	Timestamp int64
	Payload   []byte
}
