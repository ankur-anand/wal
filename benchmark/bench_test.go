package benchmark

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/ankur-anand/wal"
	"github.com/stretchr/testify/assert"
)

var walFile *wal.WAL

func init() {
	dir, _ := os.MkdirTemp("", "wal-benchmark-test")
	opts := wal.Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    wal.GB,
	}
	var err error
	walFile, err = wal.Open(opts)
	if err != nil {
		panic(err)
	}
}

func BenchmarkWAL_WriteLargeSize(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	content := []byte(strings.Repeat("X", 256*wal.KB+500))
	for i := 0; i < b.N; i++ {
		_, err := walFile.Write(content)
		assert.Nil(b, err)
	}
}

func BenchmarkWAL_Write(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := walFile.Write([]byte("Hello World"))
		assert.Nil(b, err)
	}
}

func BenchmarkWAL_WriteBatch(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 31; j++ {
			walFile.PendingWrites([]byte(strings.Repeat("X", wal.MB)))
		}
		walFile.PendingWrites([]byte(strings.Repeat("X", wal.MB)))
		pos, err := walFile.WriteAll()
		assert.Nil(b, err)
		assert.Equal(b, 32, len(pos))
	}
}

func BenchmarkWAL_Read(b *testing.B) {
	var positions []*wal.ChunkPosition
	for i := 0; i < 1000000; i++ {
		pos, err := walFile.Write([]byte("Hello World"))
		assert.Nil(b, err)
		positions = append(positions, pos)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := walFile.Read(positions[rand.Intn(len(positions))])
		assert.Nil(b, err)
	}
}

var (
	blockNumber  = uint32(123456)
	chunkOffset  = int64(9876543210)
	sinkKey      string
	sinkKeyBytes []byte
)

func makeKeyFmtSprintf(blockNumber uint32, chunkOffset int64) string {
	return fmt.Sprintf("%08x:%016x", blockNumber, uint64(chunkOffset))
}

func makeKeyManualFormat(blockNumber uint32, chunkOffset int64) string {
	var b strings.Builder
	b.Grow(25)
	_ = b.WriteByte("0"[0]) // workaround to avoid bounds check
	fmt.Fprintf(&b, "%08x:%016x", blockNumber, uint64(chunkOffset))
	return b.String()
}

func makeKeyBinaryString(blockNumber uint32, chunkOffset int64) string {
	var buf [12]byte
	binary.BigEndian.PutUint32(buf[0:4], blockNumber)
	binary.BigEndian.PutUint64(buf[4:], uint64(chunkOffset))
	return string(buf[:])
}

func BenchmarkKeyFmtSprintf(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sinkKey = makeKeyFmtSprintf(blockNumber, chunkOffset)
	}
}

func BenchmarkKeyManualFormat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sinkKey = fmt.Sprintf("%08x:%016x", blockNumber, chunkOffset) // use fmt only here
	}
}

func BenchmarkKeyBinaryString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sinkKey = makeKeyBinaryString(blockNumber, chunkOffset)
	}
}

func BenchmarkKeyAppendHex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sinkKey = makeKeyAppendHex(blockNumber, chunkOffset)
	}
}

// Fastest manual hex encoding
func makeKeyAppendHex(blockNumber uint32, chunkOffset int64) string {
	var buf [25]byte
	b := buf[:0]
	b = appendUintHex(b, uint64(blockNumber), 8)
	b = append(b, ':')
	b = appendUintHex(b, uint64(chunkOffset), 16)
	return string(b)
}

func appendUintHex(b []byte, n uint64, width int) []byte {
	s := strconv.FormatUint(n, 16)
	for i := len(s); i < width; i++ {
		b = append(b, '0')
	}
	return append(b, s...)
}
