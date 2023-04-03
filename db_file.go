package minidb

import (
	"fmt"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/yanghao888/minidb/fileutil"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	logFileNameSuffix   = ".log"
	indexFileNameSuffix = ".index"
	tempFileNameSuffix  = ".tmp"
)

type replayFn func(key []byte, lo *logOffset) error

type dbFile struct {
	dirPath string
	files   []*logFile

	maxPtr uint64
	db     *DB
	opt    Options
}

func (df *dbFile) Open(db *DB, opt Options) error {
	df.db = db
	df.opt = opt
	df.dirPath = opt.Dir
	if err := df.openOrCreateFiles(); err != nil {
		return errors.Wrapf(err, "Unable to open log file")
	}
	return nil
}

func (df *dbFile) Close() error {
	var err error
	for _, lf := range df.files {
		// A successful close does not guarantee that the data has been successfully saved to disk, as the kernel defers writes.
		// It is not common for a file system to flush the buffers when the stream is closed.
		if syncErr := fileutil.Fdatasync(lf.fd); syncErr != nil && err == nil {
			err = syncErr
		}
		if closeErr := lf.fd.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

func (df *dbFile) Replay(fn replayFn) error {
	var lastOffset uint32
	for _, lf := range df.files {
		endAt, err := df.iterate(lf, fn)
		if err != nil {
			return errors.Wrapf(err, "Unable to replay log: %q", lf.path)
		}
		if lf.fid == df.maxFid() {
			lastOffset = endAt
		}
	}

	// Seek to the end to start writing.
	last := df.files[len(df.files)-1]
	if _, err := last.fd.Seek(int64(lastOffset), io.SeekStart); err != nil {
		return errors.Wrapf(err, "Unable to seek to end of active log: %q", last.path)
	}
	atomic.AddUint64(&df.maxPtr, uint64(lastOffset))
	return nil
}

func (df *dbFile) openOrCreateFiles() error {
	files, err := os.ReadDir(df.dirPath)
	if err != nil {
		return errors.Wrapf(err, "Error while opening log file dir")
	}

	found := make(map[uint64]struct{})
	var maxFid uint32 // Beware len(files) == 0 case, this starts at 0.
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), logFileNameSuffix) {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fsz-4], 10, 32)
		if err != nil {
			return errors.Wrapf(err, "Error while parsing log file id for file: %q", file.Name())
		}
		if _, ok := found[fid]; ok {
			return errors.Errorf("Found the same log file twice: %d", fid)
		}
		found[fid] = struct{}{}

		lf := &logFile{
			fid:  uint32(fid),
			path: df.fPath(uint32(fid)),
			db:   df.db,
		}
		df.files = append(df.files, lf)
		if uint32(fid) > maxFid {
			maxFid = uint32(fid)
		}
	}
	df.maxPtr = uint64(maxFid) << 32

	// If no files are found, then create a new file.
	if len(df.files) == 0 {
		return df.createLogFile(0)
	}

	sort.Slice(df.files, func(i, j int) bool {
		return df.files[i].fid < df.files[j].fid
	})

	// Open all log files as read write.
	for i := len(df.files) - 1; i >= 0; i-- {
		lf := df.files[i]
		err = lf.openReadWrite()
		if err != nil {
			return errors.Wrapf(err, "Open existing file: %q", lf.path)
		}
		// We shouldn't delete the maxFid file.
		if lf.size == 0 && lf.fid != maxFid {
			log.Infof("Deleting empty file: %q", lf.path)
			if err = lf.delete(); err != nil {
				return errors.Wrapf(err, "Error while trying to delete empty file: %q", lf.path)
			}
			df.files = append(df.files[:i], df.files[i+1:]...)

			idxFilePath := indexFilePath(df.dirPath, lf.fid)
			log.Infof("Deleting empty file: %q", idxFilePath)
			if err = os.Remove(idxFilePath); err != nil {
				return errors.Wrapf(err, "Error while trying to delete empty file: %q", idxFilePath)
			}
		}
	}
	return nil
}

// iterate iterates over log file.
func (df *dbFile) iterate(lf *logFile, fn replayFn) (uint32, error) {
	if lf.fid != df.maxFid() {
		// Read index from hint file if the file exists
		idxFilePath := indexFilePath(df.dirPath, lf.fid)
		if fi, err := os.Stat(idxFilePath); os.IsExist(err) {
			hf := &hintFile{fid: lf.fid, size: uint32(fi.Size()), path: idxFilePath}
			if err = hf.openReadOnly(); err != nil {
				return 0, err
			}
			defer hf.close(hf.size)
			return hf.iterate(fn)
		}
	}
	return lf.iterate(fn)
}

// Read an entry from log file by logOffset. The log file may be readonly.
func (df *dbFile) Read(lo *logOffset) (e *Entry, err error) {
	lf, err := df.getFile(lo.fid)
	if err != nil {
		return nil, err
	}
	return lf.read(lo.offset)
}

// Write the entry into active log file.
func (df *dbFile) Write(e *Entry) (lo *logOffset, err error) {
	alf := df.activeLogFile()
	if alf == nil {
		return nil, errors.New("Unable to find the active log file")
	}
	err = alf.write(e)
	if err != nil {
		return nil, errors.Wrapf(err, "Error while writing log file fid %d", alf.fid)
	}
	lo = &logOffset{fid: alf.fid, offset: df.writableOffset()}
	atomic.AddUint64(&df.maxPtr, uint64(e.Size()))
	if df.writableOffset() > uint32(df.opt.LogFileSize) {
		if err = alf.doneWriting(df.writableOffset()); err != nil {
			return
		}
		if err = df.createLogFile(df.maxFid() + 1); err != nil {
			return
		}
	}
	return
}

func (df *dbFile) merge() error {
	if len(df.files) < 2 {
		return nil
	}
	// Exclude active log file.
	oldFiles := df.files[:len(df.files)-1]
	for _, lf := range oldFiles {
		if err := lf.runGc(); err != nil {
			return err
		}
	}
	return nil
}

// getFile return logFile by fid, return ErrFileNotFound
// if that logFile not found.
func (df *dbFile) getFile(fid uint32) (*logFile, error) {
	for i := len(df.files) - 1; i >= 0; i-- {
		file := df.files[i]
		if file.fid == fid {
			return file, nil
		}
	}
	return nil, ErrFileNotFound
}

func logFilePath(dirPath string, fid uint32) string {
	return fmt.Sprintf("%s%s%06d%s", dirPath, string(os.PathSeparator), fid, logFileNameSuffix)
}

func indexFilePath(dirPath string, fid uint32) string {
	return fmt.Sprintf("%s%s%06d%s", dirPath, string(os.PathSeparator), fid, indexFileNameSuffix)
}

func (df *dbFile) fPath(fid uint32) string {
	return logFilePath(df.dirPath, fid)
}

// activeLogFile return the active log file.
func (df *dbFile) activeLogFile() *logFile {
	if len(df.files) > 0 {
		return df.files[len(df.files)-1]
	}
	return nil
}

// createLogFile create a new log file replace current active log file.
func (df *dbFile) createLogFile(fid uint32) error {
	atomic.StoreUint64(&df.maxPtr, uint64(fid)<<32)

	path := df.fPath(fid)
	lf := &logFile{fid: fid, path: path, db: df.db}

	var err error
	if lf.fd, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666); err != nil {
		return errors.Wrapf(err, "Unable to create log file")
	}
	if err = lf.fd.Truncate(df.opt.LogFileSize); err != nil {
		return errors.Wrap(err, "Unable to truncate log file")
	}

	if err = syncDir(df.dirPath); err != nil {
		return errors.Wrapf(err, "Unable to sync log file dir")
	}
	df.files = append(df.files, lf)
	return nil
}

func (df *dbFile) maxFid() uint32 {
	return uint32(atomic.LoadUint64(&df.maxPtr) >> 32)
}

func (df *dbFile) writableOffset() uint32 {
	return uint32(atomic.LoadUint64(&df.maxPtr))
}

// logFile provides read and write for log entry.
type logFile struct {
	fid  uint32
	size uint32
	path string
	fd   *os.File
	db   *DB
}

func (lf *logFile) openReadWrite() error {
	return lf.open(os.O_RDWR, 0666)
}

func (lf *logFile) open(flag int, perm os.FileMode) (err error) {
	lf.fd, err = os.OpenFile(lf.path, flag, perm)
	if err != nil {
		return errors.Wrapf(err, "Unable to open %q.", lf.path)
	}

	fi, err := lf.fd.Stat()
	if err != nil {
		return errors.Wrapf(err, "Unable to check stat for %q", lf.path)
	}
	lf.size = uint32(fi.Size())
	return nil
}

func (lf *logFile) doneWriting(offset uint32) error {
	if err := lf.fd.Truncate(int64(offset)); err != nil {
		return errors.Wrapf(err, "Unable to truncate file: %q", lf.path)
	}
	if err := fileutil.Fsync(lf.fd); err != nil {
		return errors.Wrapf(err, "Unable to sync log file: %q", lf.path)
	}
	return nil
}

// delete closes the log file and remove it from FS.
func (lf *logFile) delete() error {
	if err := lf.fd.Truncate(0); err != nil {
		// This is very important to let the FS know that the file is deleted.
		return err
	}
	filename := lf.fd.Name()
	if err := lf.fd.Close(); err != nil {
		return err
	}
	return os.Remove(filename)
}

// OpenOrCreateFileWithZeroOffset Opens or create file for path, and seek start.
func OpenOrCreateFileWithZeroOffset(path string, flag int) (*os.File, uint32, error) {
	fd, err := os.OpenFile(path, flag|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Unable to create file: %q", path)
	}
	offset, err := fd.Seek(0, io.SeekStart)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Unable to seek file: %q", path)
	}
	return fd, uint32(offset), nil
}

func TruncateAndCloseFile(fd *os.File, size uint32) error {
	var err error
	filename := fd.Name()
	if err = fd.Truncate(int64(size)); err != nil {
		return errors.Wrapf(err, "Unable to truncate file: %q", filename)
	}
	if err = fileutil.Fsync(fd); err != nil {
		return errors.Wrapf(err, "Unable to sync file: %q", filename)
	}
	if err = fd.Close(); err != nil {
		return errors.Wrapf(err, "Unable to close file: %q", filename)
	}
	return nil
}

func (lf *logFile) runGc() error {
	var err error
	tempLogPath := lf.path + tempFileNameSuffix
	tmpLogFd, writableOffset, err := OpenOrCreateFileWithZeroOffset(tempLogPath, os.O_WRONLY)
	if err != nil {
		return err
	}

	idxFilePath := indexFilePath(filepath.Dir(lf.path), lf.fid)
	tempIndexPath := idxFilePath + tempFileNameSuffix
	hf := &hintFile{fid: lf.fid, path: tempIndexPath}
	if err = hf.openWriteOnly(); err != nil {
		return err
	}

	if err = syncDir(filepath.Dir(lf.path)); err != nil {
		return errors.Wrap(err, "Unable to sync log file dir")
	}

	var (
		offset    uint32
		e         *Entry
		newKeyDir = make(map[string]*logOffset)
	)
	for {
		e, err = lf.read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if e.mark == Tombstone {
			offset += e.Size()
			continue
		}
		successful, err := lf.compareAndRewrite(e, offset, tmpLogFd)
		if err != nil {
			return errors.Wrapf(err, "Unable to write entry into temp log file: %q", tempLogPath)
		}
		if successful {
			// Write index into hint file
			idx := &Index{fid: lf.fid, offset: writableOffset, kLen: e.kLen, key: e.key}
			if err = hf.write(idx); err != nil {
				return errors.Wrapf(err, "Unable to write into hint file: %q", tempIndexPath)
			}
			// Cache offset waiting for a one-time update (because the file has not been replaced)
			newKeyDir[string(e.key)] = &logOffset{fid: lf.fid, offset: writableOffset}
			writableOffset += e.Size()
		}
		offset += e.Size()
	}

	if err = TruncateAndCloseFile(tmpLogFd, writableOffset); err != nil {
		return err
	}
	if err = hf.close(hf.size); err != nil {
		return err
	}

	// Replace log file and update keyDir
	db := lf.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if err = lf.delete(); err != nil {
		return err
	}
	if err = os.Rename(tempLogPath, lf.path); err != nil {
		return err
	}
	if err = lf.openReadWrite(); err != nil {
		return err
	}
	db.updateKeyDir(newKeyDir)

	if err = os.Rename(tempIndexPath, idxFilePath); err != nil {
		return err
	}

	return nil
}

func (lf *logFile) compareAndRewrite(e *Entry, offset uint32, fd *os.File) (bool, error) {
	db := lf.db
	db.mu.RLock()
	defer db.mu.RUnlock()

	if lo, has := db.keyDir[string(e.key)]; has && lo.fid == lf.fid && lo.offset == offset {
		bytes, err := encodeEntry(e)
		if err != nil {
			return false, err
		}
		// Write entry to temp log file
		if _, err = fd.Write(bytes); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// write the entry in log file.
func (lf *logFile) write(e *Entry) error {
	bytes, err := encodeEntry(e)
	if err != nil {
		return err
	}
	if _, err = lf.fd.Write(bytes); err != nil {
		return err
	}
	return nil
}

// readWithSize reads entry from log file.
func (lf *logFile) readWithSize(offset, n uint32) (*Entry, error) {
	buf := make([]byte, n)
	if _, err := lf.fd.ReadAt(buf, int64(offset)); err != nil && err != io.EOF {
		return nil, err
	}
	return decodeEntry(buf)
}

// read entry from log file.
func (lf *logFile) read(offset uint32) (*Entry, error) {
	buf := make([]byte, entryHeaderSize)
	if _, err := lf.fd.ReadAt(buf, int64(offset)); err != nil {
		return nil, err
	}
	e, err := decodeEntry(buf)
	if err != nil {
		return nil, err
	}
	if n := e.kLen + e.vLen; n > 0 {
		if n > entryHeaderSize {
			buf = make([]byte, n)
		} else {
			buf = buf[:n]
		}
		offset += entryHeaderSize
		if _, err = lf.fd.ReadAt(buf, int64(offset)); err != nil {
			return nil, err
		}
		e.key = make([]byte, e.kLen)
		e.value = make([]byte, e.vLen)
		copy(e.key, buf[:e.kLen])
		copy(e.value, buf[e.kLen:])
	}
	return e, nil
}

func (lf *logFile) iterate(fn replayFn) (uint32, error) {
	var offset uint32
	for {
		e, err := lf.read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		if e.mark == Tombstone {
			if err = fn(e.key, nil); err != nil {
				return 0, err
			}
			offset += e.Size()
			continue
		}
		// The length of key cannot be zero unless the log file is not filled with actual data
		if e.kLen == 0 {
			break
		}
		if err = fn(e.key, &logOffset{fid: lf.fid, offset: offset}); err != nil {
			return 0, err
		}
		offset += e.Size()
	}
	return offset, nil
}

// hintFile provides read and write for log index.
type hintFile struct {
	fid  uint32
	size uint32
	path string
	fd   *os.File
}

func (hf *hintFile) openReadOnly() error {
	return hf.openOrCreate(os.O_RDONLY, 0666)
}

func (hf *hintFile) openWriteOnly() error {
	return hf.openOrCreate(os.O_WRONLY, 0666)
}

func (hf *hintFile) openOrCreate(flag int, perm os.FileMode) (err error) {
	hf.fd, err = os.OpenFile(hf.path, flag|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return errors.Wrapf(err, "Unable to open or create file: %q.", hf.path)
	}

	_, err = hf.fd.Seek(0, io.SeekStart)
	if err != nil {
		return errors.Wrapf(err, "Unable to seek file: %q", hf.path)
	}
	return nil
}

func (hf *hintFile) close(size uint32) error {
	var err error
	filename := hf.fd.Name()
	if err = hf.fd.Truncate(int64(size)); err != nil {
		return errors.Wrapf(err, "Unable to truncate file: %q", filename)
	}
	if err = fileutil.Fsync(hf.fd); err != nil {
		return errors.Wrapf(err, "Unable to sync file: %q", filename)
	}
	if err = hf.fd.Close(); err != nil {
		return errors.Wrapf(err, "Unable to close file: %q", filename)
	}
	return nil
}

func (hf *hintFile) write(idx *Index) error {
	bytes, err := encodeIndex(idx)
	if err != nil {
		return err
	}
	if _, err = hf.fd.Write(bytes); err != nil {
		return err
	}
	hf.size += idx.Size()
	return nil
}

func (hf *hintFile) iterate(fn replayFn) (uint32, error) {
	var lastOffset uint32
	buf := make([]byte, indexHeaderSize)
	for {
		if _, err := hf.fd.Read(buf); err != nil {
			if err == io.EOF {
				break
			}
			return 0, errors.Wrapf(err, "Unable to read file: %q", hf.path)
		}
		idx, err := decodeIndex(buf)
		if err != nil {
			return 0, err
		}
		idx.key = make([]byte, idx.kLen)
		if _, err = hf.fd.Read(idx.key); err != nil {
			if err == io.EOF {
				break
			}
			return 0, errors.Wrapf(err, "Unable to read file: %q", hf.path)
		}
		if err = fn(idx.key, &logOffset{fid: idx.fid, offset: idx.offset}); err != nil {
			return 0, err
		}
		if idx.offset <= lastOffset {
			return 0, errors.Errorf("Error offset, idx.offset: %d, lastOffset: %d", idx.offset, lastOffset)
		}
		lastOffset = idx.offset
	}
	return lastOffset, nil
}
