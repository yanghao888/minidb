package minidb

// Options are params for creating DB object.
type Options struct {

	// ----------------------------- //
	//        Mandatory flags        //
	// ----------------------------- //

	// Directory to store the data in.
	Dir string

	// ----------------------------- //
	//   Frequently modified flags   //
	// ----------------------------- //

	// Size of single log file.
	LogFileSize int64
}

// DefaultOptions sets a list of recommended options for good performance.
// Feel free to modify these to suit your needs.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:         dir,
		LogFileSize: 256 << 20,
	}
}
