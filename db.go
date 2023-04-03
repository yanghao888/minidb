package minidb

import (
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"os"
	"sync"
	"sync/atomic"
)

const (
	lockFile = "LOCK"
)

type DB struct {
	mu           sync.RWMutex
	dirLockGuard *directoryLockGuard

	opt    Options
	keyDir map[string]*logOffset
	dbFile dbFile
	closed atomic.Bool
	gcLock sync.Mutex
}

// Open return a new DB instance.
func Open(opt Options) (*DB, error) {
	if _, err := os.Stat(opt.Dir); err != nil {
		if !os.IsNotExist(err) {
			return nil, errors.Wrapf(err, "Invalid Dir: %q", opt.Dir)
		}
		if err = os.MkdirAll(opt.Dir, 0700); err != nil && !os.IsExist(err) {
			return nil, errors.Wrapf(err, "Unable to create dir: %q", opt.Dir)
		}
	}

	dirLockGuard, err := acquireDirectoryLock(opt.Dir, lockFile)
	if err != nil {
		return nil, err
	}

	if opt.LogFileSize < 1<<20 || opt.LogFileSize > 2<<30 {
		return nil, ErrLogFileSize
	}

	db := &DB{
		dirLockGuard: dirLockGuard,
		opt:          opt,
		keyDir:       make(map[string]*logOffset),
	}

	log.Info("Database opening")
	if err := db.dbFile.Open(db, opt); err != nil {
		return nil, err
	}

	// Replay log file or hint file
	err = db.dbFile.Replay(func(key []byte, lo *logOffset) error {
		db.keyDir[string(key)] = lo
		return nil
	})
	if err != nil {
		return nil, err
	}
	log.Info("Database opened")
	return db, nil
}

// When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes).  (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func syncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory: %s", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s", dir)
}

// Put adds a key-value pair to the database.
func (db *DB) Put(key, val []byte) (err error) {
	if db.isClosed() {
		return ErrDatabaseClosed
	}
	if len(key) == 0 {
		return ErrEmptyKey
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Write to file
	e := NewEntry(key, val, Normal)
	lo, err := db.dbFile.Write(e)
	if err != nil {
		return err
	}

	// Update index
	db.keyDir[string(key)] = lo

	return
}

// Get looks for key and returns corresponding Item.
// If key is not found, ErrKeyNotFound is returned.
func (db *DB) Get(key []byte) ([]byte, error) {
	if db.isClosed() {
		return nil, ErrDatabaseClosed
	}
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	lo, ok := db.keyDir[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	e, err := db.dbFile.Read(lo)
	if err != nil {
		return nil, err
	}
	return e.value, nil
}

// Delete deletes a key. This is done by adding a deleted marker for the key.
func (db *DB) Delete(key []byte) (err error) {
	if db.isClosed() {
		return ErrDatabaseClosed
	}
	if len(key) == 0 {
		return ErrEmptyKey
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Search for key
	if _, ok := db.keyDir[string(key)]; !ok {
		return
	}

	// Write to file
	e := NewEntry(key, nil, Tombstone)
	_, err = db.dbFile.Write(e)
	if err != nil {
		return
	}

	// Delete index (possible memory leak because the map does not shrink)
	delete(db.keyDir, string(key))

	return
}

// Merge cleans old log file and rewrite key-value pair index.
func (db *DB) Merge() error {
	if !db.gcLock.TryLock() {
		return ErrGcWorking
	}
	defer db.gcLock.Unlock()
	return db.dbFile.merge()
}

func (db *DB) updateKeyDir(m map[string]*logOffset) {
	if len(m) == 0 {
		return
	}
	for key, newOffset := range m {
		// Confirm that the key has not been modified
		if curOffset, has := db.keyDir[key]; has && curOffset.fid == newOffset.fid {
			db.keyDir[key] = newOffset
		}
	}
}

// Close an opened DB instance.
func (db *DB) Close() (err error) {
	if db.isClosed() {
		log.Warn("Database has already closed")
		return
	}
	log.Info("Database closing")

	if dbFileErr := db.dbFile.Close(); err == nil {
		err = errors.Wrap(dbFileErr, "DB.Close")
	}

	if db.dirLockGuard != nil {
		if guardErr := db.dirLockGuard.release(); err == nil {
			err = errors.Wrap(guardErr, "DB.Close")
		}
	}

	// Fsync directories to ensure that lock file, and any other removed files whose directory
	// we haven't specifically fsynced, are guaranteed to have their directory entry removal
	// persisted to disk.
	if syncErr := syncDir(db.opt.Dir); err == nil {
		err = errors.Wrap(syncErr, "DB.Close")
	}

	db.closed.CompareAndSwap(false, true)
	db.keyDir = nil
	log.Info("Database closed")
	return err
}

func (db *DB) isClosed() bool {
	return db.closed.Load()
}
