package wal

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func destroyWAL(wal *WAL) {
	if wal != nil {
		_ = wal.Close()
		_ = os.RemoveAll(wal.options.DirPath)
	}
}

func TestWAL_WriteALL(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write-batch-1")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAllIterate(t, wal, 0, 10)
	assert.True(t, wal.IsEmpty())

	testWriteAllIterate(t, wal, 10000, 512)
	assert.False(t, wal.IsEmpty())
}

func TestWAL_Write(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write1")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	// write 1
	pos1, err := wal.Write([]byte("hello1"))
	assert.Nil(t, err)
	assert.NotNil(t, pos1)
	pos2, err := wal.Write([]byte("hello2"))
	assert.Nil(t, err)
	assert.NotNil(t, pos2)
	pos3, err := wal.Write([]byte("hello3"))
	assert.Nil(t, err)
	assert.NotNil(t, pos3)

	val, err := wal.Read(pos1)
	assert.Nil(t, err)
	assert.Equal(t, "hello1", string(val))
	val, err = wal.Read(pos2)
	assert.Nil(t, err)
	assert.Equal(t, "hello2", string(val))
	val, err = wal.Read(pos3)
	assert.Nil(t, err)
	assert.Equal(t, "hello3", string(val))
}

func TestWAL_Write_large(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write2")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 100000, 512)
}

func TestWAL_Write_large2(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write3")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 2000, 32*1024*3+10)
}

func TestWAL_OpenNewActiveSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-new-active-segment")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 2000, 512)
	err = wal.OpenNewActiveSegment()
	assert.Nil(t, err)

	val := strings.Repeat("wal", 100)
	for i := 0; i < 100; i++ {
		pos, err := wal.Write([]byte(val))
		assert.Nil(t, err)
		assert.NotNil(t, pos)
	}
}

func TestWAL_IsEmpty(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-is-empty")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	assert.True(t, wal.IsEmpty())
	testWriteAndIterate(t, wal, 2000, 512)
	assert.False(t, wal.IsEmpty())
}

func TestWAL_Reader(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-wal-reader")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	var size = 100000
	val := strings.Repeat("wal", 512)
	for i := 0; i < size; i++ {
		_, err := wal.Write([]byte(val))
		assert.Nil(t, err)
	}

	validate := func(walInner *WAL, size int) {
		var i = 0
		reader := walInner.NewReader()
		for {
			chunk, position, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}
			assert.NotNil(t, chunk)
			assert.NotNil(t, position)
			assert.Equal(t, position.SegmentId, reader.CurrentSegmentId())
			i++
		}
		assert.Equal(t, i, size)
	}

	validate(wal, size)
	err = wal.Close()
	assert.Nil(t, err)

	wal2, err := Open(opts)
	assert.Nil(t, err)
	defer func() {
		_ = wal2.Close()
	}()
	validate(wal2, size)
}

func testWriteAllIterate(t *testing.T, wal *WAL, size, valueSize int) {
	for i := 0; i < size; i++ {
		val := strings.Repeat("wal", valueSize)
		wal.PendingWrites([]byte(val))
	}
	positions, err := wal.WriteAll()
	assert.Nil(t, err)
	assert.Equal(t, len(positions), size)

	count := 0
	reader := wal.NewReader()
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break
		}
		assert.Equal(t, strings.Repeat("wal", valueSize), string(data))

		assert.Equal(t, positions[count].SegmentId, pos.SegmentId)
		assert.Equal(t, positions[count].BlockNumber, pos.BlockNumber)
		assert.Equal(t, positions[count].ChunkOffset, pos.ChunkOffset)

		count++
	}
	assert.Equal(t, len(wal.pendingWrites), 0)
}

func testWriteAndIterate(t *testing.T, wal *WAL, size int, valueSize int) {
	val := strings.Repeat("wal", valueSize)
	positions := make([]*ChunkPosition, size)
	for i := 0; i < size; i++ {
		pos, err := wal.Write([]byte(val))
		assert.Nil(t, err)
		positions[i] = pos
	}

	var count int
	// iterates all the data
	reader := wal.NewReader()
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break
		}
		assert.Equal(t, val, string(data))

		assert.Equal(t, positions[count].SegmentId, pos.SegmentId)
		assert.Equal(t, positions[count].BlockNumber, pos.BlockNumber)
		assert.Equal(t, positions[count].ChunkOffset, pos.ChunkOffset)

		count++
	}
	assert.Equal(t, size, count)
}

func TestWAL_Delete(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-delete")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	testWriteAndIterate(t, wal, 2000, 512)
	assert.False(t, wal.IsEmpty())
	defer destroyWAL(wal)

	err = wal.Delete()
	assert.Nil(t, err)

	wal, err = Open(opts)
	assert.Nil(t, err)
	assert.True(t, wal.IsEmpty())
}

func TestWAL_ReaderWithStart(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-wal-reader-with-start")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    8 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	_, err = wal.NewReaderWithStart(nil)
	assert.NotNil(t, err)

	reader1, err := wal.NewReaderWithStart(&ChunkPosition{SegmentId: 0, BlockNumber: 0, ChunkOffset: 100})
	assert.Nil(t, err)
	_, _, err = reader1.Next()
	assert.Equal(t, err, io.EOF)

	testWriteAndIterate(t, wal, 20000, 512)
	reader2, err := wal.NewReaderWithStart(&ChunkPosition{SegmentId: 0, BlockNumber: 0, ChunkOffset: 0})
	assert.Nil(t, err)
	_, pos2, err := reader2.Next()
	assert.Nil(t, err)
	assert.Equal(t, pos2.BlockNumber, uint32(0))
	assert.Equal(t, pos2.ChunkOffset, int64(0))

	reader3, err := wal.NewReaderWithStart(&ChunkPosition{SegmentId: 3, BlockNumber: 5, ChunkOffset: 0})
	assert.Nil(t, err)
	_, pos3, err := reader3.Next()
	assert.Nil(t, err)
	assert.Equal(t, pos3.SegmentId, uint32(3))
	assert.Equal(t, pos3.BlockNumber, uint32(5))
}

func TestWAL_RenameFileExt(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-rename-ext")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".VLOG.1.temp",
		SegmentSize:    8 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)
	testWriteAndIterate(t, wal, 20000, 512)

	err = wal.Close()
	assert.Nil(t, err)

	err = wal.RenameFileExt(".VLOG.1")
	assert.Nil(t, err)

	opts.SegmentFileExt = ".VLOG.1"
	wal2, err := Open(opts)
	assert.Nil(t, err)
	defer func() {
		_ = wal2.Close()
	}()
	for i := 0; i < 20000; i++ {
		_, err = wal2.Write([]byte(strings.Repeat("W", 512)))
		assert.Nil(t, err)
	}
}

func TestParallelReadWrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_race_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := Options{
		DirPath:        dir,
		SegmentSize:    32 * 1024 * 1024,
		SegmentFileExt: ".wal",
		SyncInterval:   time.Millisecond * 100,
		Sync:           false,
		BytesPerSync:   0,
	}

	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("error closing WAL: %v", err)
		}
		if err := w.Delete(); err != nil {
			t.Errorf("error deleting WAL: %v", err)
		}
	}()

	const (
		numWriters       = 8
		numReaders       = 16
		entriesPerWriter = 1000
		maxEntrySize     = 4096
	)

	var (
		writtenData      = make([][][]byte, numWriters)
		writtenPositions = make([][]*ChunkPosition, numWriters)
		writtenCount     atomic.Int32
		readCount        atomic.Int32
		writeErrors      atomic.Int32
		readErrors       atomic.Int32
		writerWg         sync.WaitGroup
		readerWg         sync.WaitGroup
	)

	// Generate random data for each writer
	initialRnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numWriters; i++ {
		writtenData[i] = make([][]byte, entriesPerWriter)
		writtenPositions[i] = make([]*ChunkPosition, entriesPerWriter)

		for j := 0; j < entriesPerWriter; j++ {
			size := initialRnd.Intn(maxEntrySize) + 1 // Ensure at least 1 byte
			data := make([]byte, size)
			initialRnd.Read(data)
			writtenData[i][j] = data
		}
	}

	writerDone := make(chan struct{})
	for i := 0; i < numWriters; i++ {
		writerWg.Add(1)
		go func(writerID int) {
			defer writerWg.Done()

			localRnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(writerID)))

			for j := 0; j < entriesPerWriter; j++ {
				data := writtenData[writerID][j]
				pos, err := w.Write(data)
				if err != nil {
					t.Logf("Writer %d failed to write entry %d: %v\n", writerID, j, err)
					writeErrors.Add(1)
					continue
				}

				writtenPositions[writerID][j] = pos
				writtenCount.Add(1)

				if localRnd.Intn(100) < 5 {
					time.Sleep(time.Millisecond * time.Duration(localRnd.Intn(5)))
				}
			}
		}(i)
	}

	time.Sleep(50 * time.Millisecond)

	for i := 0; i < numReaders; i++ {
		readerWg.Add(1)
		go func(readerID int) {
			defer readerWg.Done()

			localRnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(readerID)))
			for {
				select {
				case <-writerDone:
					return
				default:

					reader := w.NewReader()

					for {
						data, pos, err := reader.Next()
						if err == io.EOF {
							break
						}
						if err != nil {
							readErrors.Add(1)
							break
						}

						validationData, err := w.Read(pos)
						if err != nil {
							readErrors.Add(1)
							continue
						}

						if !bytes.Equal(data, validationData) {
							t.Errorf("Data inconsistency: Next() returned different data than Read()")
							readErrors.Add(1)
						}

						readCount.Add(1)
					}

					time.Sleep(time.Millisecond * time.Duration(localRnd.Intn(10)))
				}
			}
		}(i)
	}

	writerWg.Wait()
	close(writerDone)

	readerWg.Wait()

	var validationErrors int32

	for writerID := 0; writerID < numWriters; writerID++ {
		for j := 0; j < entriesPerWriter; j++ {
			if writtenPositions[writerID][j] == nil {
				continue
			}

			data, err := w.Read(writtenPositions[writerID][j])
			if err != nil {
				t.Errorf("Failed to read entry %d from writer %d: %v", j, writerID, err)
				validationErrors++
				continue
			}

			if !bytes.Equal(data, writtenData[writerID][j]) {
				t.Errorf("Data mismatch for entry %d from writer %d", j, writerID)
				validationErrors++
			}
		}
	}

	if writeErrors.Load() > 0 || readErrors.Load() > 0 || validationErrors > 0 {
		t.Errorf("Test failed with errors")
	}
}

func TestRotationRaceCondition(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_rotation_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := Options{
		DirPath:        dir,
		SegmentSize:    64 * 1024,
		SegmentFileExt: ".wal",
		SyncInterval:   time.Millisecond * 100,
		Sync:           false,
		BytesPerSync:   0,
	}

	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("error closing WAL: %v", err)
		}
		if err := w.Delete(); err != nil {
			t.Errorf("error deleting WAL: %v", err)
		}
	}()

	const (
		numWriters    = 4
		numReaders    = 8
		writeDuration = 2 * time.Second
		entrySize     = 1024 // 1KB entries
	)

	var (
		writeCount    atomic.Int32
		readCount     atomic.Int32
		rotationCount atomic.Uint32
		readErrors    atomic.Int32
		wg            sync.WaitGroup
		done          = make(chan struct{})
	)

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(writerID)))

			for {
				select {
				case <-done:
					return
				default:

					count := writeCount.Add(1)
					data := make([]byte, entrySize)
					rnd.Read(data)

					copy(data[0:8], fmt.Sprintf("%04d%04d", writerID, count%10000))

					_, err := w.Write(data)
					if err != nil {
						t.Errorf("Write failed: %v", err)
					}

					currentSegmentID := w.ActiveSegmentID()
					if currentSegmentID > rotationCount.Load() {
						rotationCount.Store(currentSegmentID)
					}

					time.Sleep(time.Microsecond * time.Duration(rnd.Intn(500)))
				}
			}
		}(i)
	}

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
					reader := w.NewReader()

					for {
						data, _, err := reader.Next()
						if err == io.EOF {
							break
						}
						if err != nil {
							readErrors.Add(1)
							break
						}

						if len(data) < 8 {
							t.Errorf("Read corrupted data (too short): %v", data)
							readErrors.Add(1)
						}

						readCount.Add(1)
					}

					time.Sleep(time.Millisecond * 5)
				}
			}
		}(i)
	}

	time.Sleep(writeDuration)
	close(done)

	wg.Wait()

	if readErrors.Load() > 0 {
		t.Errorf("Test failed with errors")
	}

	if rotationCount.Load() < 2 {
		t.Errorf("Expected multiple segment rotations, but only got %d", rotationCount.Load())
	}
}

func TestConcurrentReaderCreation(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_reader_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := Options{
		DirPath:        dir,
		SegmentSize:    1 * 1024 * 1024,
		SegmentFileExt: ".wal",
		SyncInterval:   0,
		Sync:           false,
	}

	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("error closing WAL: %v", err)
		}
		if err := w.Delete(); err != nil {
			t.Errorf("error deleting WAL: %v", err)
		}
	}()

	const entriesCount = 1000
	positions := make([]*ChunkPosition, entriesCount)

	for i := 0; i < entriesCount; i++ {
		data := []byte(fmt.Sprintf("test data entry %d", i))
		pos, err := w.Write(data)
		if err != nil {
			t.Fatal(err)
		}
		positions[i] = pos
	}

	const (
		numReaderGoroutines = 100
		iterationsPerReader = 50
	)

	var wg sync.WaitGroup
	readerErrors := atomic.Int32{}

	for i := 0; i < numReaderGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			for j := 0; j < iterationsPerReader; j++ {

				var reader *Reader
				var err error

				switch rnd.Intn(3) {
				case 0:
					reader = w.NewReader()
				case 1:
					maxSegId := w.ActiveSegmentID()
					reader = w.NewReaderWithMax(maxSegId)
				case 2:
					randIdx := rnd.Intn(entriesCount)
					reader, err = w.NewReaderWithStart(positions[randIdx])
					if err != nil {
						readerErrors.Add(1)
						continue
					}
				}

				readCount := 0
				for readCount < 10 {
					_, _, err := reader.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						readerErrors.Add(1)
						break
					}
					readCount++
				}

				if rnd.Intn(10) == 0 {
					data := []byte(fmt.Sprintf("concurrent write during reading %d-%d", id, j))
					_, err := w.Write(data)
					if err != nil {
						t.Errorf("Write failed during concurrent reading: %v", err)
					}
				}

				if rnd.Intn(5) == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}(i)
	}

	wg.Wait()

	if readerErrors.Load() > 0 {
		t.Errorf("Test failed with %d reader errors", readerErrors.Load())
	}
}

func TestConcurrentReadWriteActiveSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-active-segment-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := Options{
		DirPath:        dir,
		SegmentSize:    512 * 1024,
		SegmentFileExt: ".wal",
		SyncInterval:   0,
		Sync:           false,
	}

	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("error closing WAL: %v", err)
		}
	}()

	const (
		numWriters       = 4
		numReaders       = 8
		entriesPerWriter = 200
		maxEntrySize     = 1024
	)

	var (
		writeCompleted atomic.Bool
		readCount      atomic.Int32
		writeCount     atomic.Int32
		readErrors     atomic.Int32
		wg             sync.WaitGroup
	)

	var positionsMu sync.Mutex
	positions := make([]*ChunkPosition, 0, numWriters*entriesPerWriter)

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(writerID)))

			for j := 0; j < entriesPerWriter; j++ {

				size := rnd.Intn(maxEntrySize) + 100
				data := make([]byte, size)
				rnd.Read(data)

				header := fmt.Sprintf("w%d-%d", writerID, j)
				copy(data[:len(header)], []byte(header))

				pos, err := w.Write(data)
				if err != nil {
					t.Errorf("Writer %d failed to write entry %d: %v", writerID, j, err)
					continue
				}

				positionsMu.Lock()
				positions = append(positions, pos)
				positionsMu.Unlock()

				writeCount.Add(1)

				if rnd.Intn(10) == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}(i)
	}

	time.Sleep(10 * time.Millisecond)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for !writeCompleted.Load() {

				reader := w.NewReader()

				for {
					data, pos, err := reader.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						readErrors.Add(1)
						t.Logf("Reader %d encountered error: %v", readerID, err)
						break
					}

					if len(data) > 2 && data[0] == 'w' && data[1] >= '0' && data[1] <= '9' {
						readCount.Add(1)
					} else {
						t.Errorf("Invalid data read: %v", data[:10])
						readErrors.Add(1)
					}

					directData, err := w.Read(pos)
					if err != nil {
						t.Errorf("Failed to read data directly using position: %v", err)
						readErrors.Add(1)
					} else if !bytes.Equal(data, directData) {
						t.Errorf("Direct read data doesn't match reader data")
						readErrors.Add(1)
					}
				}

				time.Sleep(time.Millisecond * 5)
			}
		}(i)
	}

	go func() {
		for {
			if writeCount.Load() >= int32(numWriters*entriesPerWriter) {
				writeCompleted.Store(true)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()

	var verificationErrors int

	for i, pos := range positions {
		data, err := w.Read(pos)
		if err != nil {
			t.Errorf("Failed to read entry %d: %v", i, err)
			verificationErrors++
			continue
		}

		if len(data) < 2 || data[0] != 'w' {
			t.Errorf("Entry %d has invalid format: %v", i, data[:10])
			verificationErrors++
		}
	}

	if readErrors.Load() > 0 || verificationErrors > 0 {
		t.Errorf("Test failed with errors")
	}
}

func TestEOFOnActiveSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-eof-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := Options{
		DirPath:        dir,
		SegmentSize:    256 * 1024,
		SegmentFileExt: ".wal",
	}

	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("error closing WAL: %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("error removing test directory: %v", err)
		}
	}()

	t.Run("EmptyWAL", func(t *testing.T) {
		reader := w.NewReader()
		_, _, err := reader.Next()
		assert.Equal(t, io.EOF, err, "Expected EOF on empty WAL")
	})

	t.Run("ReaderGetsEOFAfterAllEntries", func(t *testing.T) {
		numEntries := 10
		for i := 0; i < numEntries; i++ {
			data := []byte(fmt.Sprintf("test-entry-%d", i))
			_, err := w.Write(data)
			assert.Nil(t, err)
		}

		reader := w.NewReader()
		entriesRead := 0
		for {
			_, _, err := reader.Next()
			if err == io.EOF {
				break
			}
			assert.Nil(t, err)
			entriesRead++
		}

		assert.Equal(t, numEntries, entriesRead, "Should have read exactly the number of entries written")

		_, _, err = reader.Next()
		assert.Equal(t, io.EOF, err, "Expected EOF after reading all entries")
	})

	t.Run("ConcurrentReadsAndWrites", func(t *testing.T) {
		var (
			wg           sync.WaitGroup
			writing      atomic.Bool
			readCycles   atomic.Int32
			eofCount     atomic.Int32
			entriesAdded atomic.Int32
		)

		writing.Store(true)

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer writing.Store(false)

			for i := 0; i < 50; i++ {
				data := []byte(fmt.Sprintf("concurrent-entry-%d", i))
				_, err := w.Write(data)
				assert.Nil(t, err)
				entriesAdded.Add(1)

				time.Sleep(time.Millisecond * time.Duration(rand.Intn(5)+1))
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			for writing.Load() || entriesAdded.Load() > 0 {
				reader := w.NewReader()
				entriesInCycle := 0

				for {
					_, _, err := reader.Next()
					if err == io.EOF {
						eofCount.Add(1)
						break
					}
					assert.Nil(t, err)
					entriesInCycle++
					entriesAdded.Add(-1)
				}

				readCycles.Add(1)

				time.Sleep(time.Millisecond * 10)
			}
		}()

		wg.Wait()
		assert.True(t, eofCount.Load() >= readCycles.Load(),
			"Each read cycle should encounter at least one EOF")
	})
}
