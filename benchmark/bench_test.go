package benchmark

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/yanghao888/minidb"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

var (
	ctx           = context.Background()
	flagDir       = flag.String("dir", "minidb-bench", "Where data is temporarily stored.")
	flagKeySize   = flag.Int("key_sz", 32, "Size of each key.")
	flagValueSize = flag.Int("val_sz", 128, "Size of each value.")
)

const chars string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var (
	charLen   = len(chars)
	keyPatten = "%0" + strconv.Itoa(*flagKeySize) + "d"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func getKey(i int) []byte {
	key := fmt.Sprintf(keyPatten, i)
	return []byte(key)
}

func getValue() []byte {
	var buf bytes.Buffer
	for i := 0; i < *flagValueSize; i++ {
		buf.WriteByte(chars[rand.Int()%charLen])
	}
	return buf.Bytes()
}

func runBench(b *testing.B, benchFn func(b *testing.B, db *minidb.DB)) {
	opts := minidb.DefaultOptions(*flagDir)
	db, err := minidb.Open(opts)
	assert.NoError(b, err)
	defer os.RemoveAll(*flagDir)
	defer db.Close()
	benchFn(b, db)
}

func initData(db *minidb.DB, n int) error {
	for i := 0; i < n; i++ {
		err := db.Put(getKey(i), getValue())
		if err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkDB_Put(b *testing.B) {
	runBench(b, func(b *testing.B, db *minidb.DB) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			err := db.Put(getKey(i), getValue())
			assert.NoError(b, err)
		}
	})
}

func BenchmarkDB_Get(b *testing.B) {
	runBench(b, func(b *testing.B, db *minidb.DB) {
		assert.NoError(b, initData(db, b.N))
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := db.Get(getKey(i))
			assert.NoError(b, err)
		}
	})
}
