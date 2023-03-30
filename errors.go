package minidb

import "github.com/pingcap/errors"

var (
	// ErrLogFileSize is returned when "opt.LogFileSize" option is not within the valid range.
	ErrLogFileSize = errors.New("Invalid LogFileSize, must be between 1MB and 2GB")

	ErrDatabaseClosed = errors.New("Database already closed")

	ErrEmptyKey = errors.New("Key cannot be empty")

	ErrKeyNotFound = errors.New("Key not found")

	ErrFileNotFound = errors.New("File not found")

	ErrGcWorking = errors.New("Gc is working")
)
