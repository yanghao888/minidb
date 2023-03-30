package minidb

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func getTestOptions(dir string) Options {
	opts := DefaultOptions(dir)
	return opts
}

// Opens a mini db and runs a test on it.
func runTest(t *testing.T, opts *Options, test func(t *testing.T, db *DB)) {
	dir, err := os.MkdirTemp("", "minidb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	if opts == nil {
		opts = new(Options)
		*opts = getTestOptions(dir)
	}

	db, err := Open(*opts)
	require.NoError(t, err)
	defer db.Close()
	test(t, db)
}

func TestDB_Put(t *testing.T) {
	runTest(t, nil, func(t *testing.T, db *DB) {
		n := 1000
		for i := 0; i < n; i++ {
			err := db.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
			require.NoError(t, err)
		}
		require.Equal(t, n, len(db.keyDir))
	})
}

func TestDB_Delete(t *testing.T) {
	runTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 1000; i++ {
			// Simulate that key already exist
			db.keyDir[fmt.Sprintf("key%d", i)] = &logOffset{}

			// Delete the key
			err := db.Delete([]byte(fmt.Sprintf("key%d", i)))
			require.NoError(t, err)

			require.Equal(t, 0, len(db.keyDir))
		}
	})
}

func TestDB_Get(t *testing.T) {
	dir, err := os.MkdirTemp("", "minidb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := getTestOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)
	defer func(db *DB) {
		if db.isClosed() {
			return
		}
		require.NoError(t, db.Close())
	}(db)

	const n = 1000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
	}
	for i := 0; i < n+100; i++ {
		val, err := db.Get([]byte(fmt.Sprintf("key%d", i)))
		if i < n {
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("val%d", i)), val)
		} else {
			require.Equal(t, ErrKeyNotFound, err)
		}
	}

	db.Put([]byte("keyA"), []byte("valA"))
	db.Put([]byte("keyB"), []byte("valB"))

	// The value should be obtained successfully
	val, err := db.Get([]byte("keyA"))
	require.NoError(t, err)
	require.Equal(t, []byte("valA"), val)

	// Delete the key
	db.Delete([]byte("keyA"))

	// ErrKeyNotFound should be returned
	val, err = db.Get([]byte("keyA"))
	require.Equal(t, ErrKeyNotFound, err)

	require.NoError(t, db.Close())

	// Reopen database
	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// It should still be able to get the value normally
	val, err = db.Get([]byte("keyB"))
	require.NoError(t, err)
	require.Equal(t, []byte("valB"), val)
}

func TestDB_Merge(t *testing.T) {
	dir, err := os.MkdirTemp("", "minidb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := getTestOptions(dir)
	opts.LogFileSize = 1 << 20
	db, err := Open(opts)
	require.NoError(t, err)
	defer func(db *DB) {
		if db.isClosed() {
			return
		}
		require.NoError(t, db.Close())
	}(db)

	var (
		keySize            = 16 * 1024
		valSize            = 32 * 1024
		normalEntrySize    = entryHeaderSize + keySize + valSize
		tombstoneEntrySize = entryHeaderSize + keySize
		numPut             = 100
		numDel             = 60
		numTotalFiles      = int(math.Ceil(float64(numPut*normalEntrySize+numDel*tombstoneEntrySize) / float64(opts.LogFileSize)))
		keyFormat          = "%0" + strconv.Itoa(keySize) + "d"
		valFormat          = "%0" + strconv.Itoa(valSize) + "d"
	)

	for i := 0; i < numPut; i++ {
		db.Put([]byte(fmt.Sprintf(keyFormat, i)), []byte(fmt.Sprintf(valFormat, i)))
		if i < numDel {
			db.Delete([]byte(fmt.Sprintf(keyFormat, i)))
		}
	}

	require.NoError(t, db.Merge())

	var (
		numLogFiles  int
		numHintFiles int
	)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := filepath.Ext(entry.Name())
		if ext == logFileNameSuffix {
			numLogFiles++
			continue
		}
		if ext == indexFileNameSuffix {
			numHintFiles++
		}
	}
	require.Equal(t, numTotalFiles, numLogFiles)
	// Only old log files has hint file
	require.Equal(t, numTotalFiles-1, numHintFiles)

	require.NoError(t, db.Close())

	// Reopen database
	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()

	numLogFiles, numHintFiles = 0, 0
	entries, err = os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := filepath.Ext(entry.Name())
		if ext == logFileNameSuffix {
			numLogFiles++
			continue
		}
		if ext == indexFileNameSuffix {
			numHintFiles++
		}
	}
	expectedNumLogFiles := float64(numTotalFiles) - math.Floor(float64(numDel*(normalEntrySize+tombstoneEntrySize))/float64(opts.LogFileSize))
	require.EqualValues(t, expectedNumLogFiles, numLogFiles)
	require.Equal(t, numLogFiles-1, numHintFiles)

	// It should still be able to get the value normally
	for i := 0; i < numPut; i++ {
		val, err := db.Get([]byte(fmt.Sprintf(keyFormat, i)))
		if i < numDel {
			require.Equal(t, ErrKeyNotFound, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf(valFormat, i)), val)
		}
	}
}
