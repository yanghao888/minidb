package minidb

const (
	entryHeaderSize = 9
	indexHeaderSize = 12
)

type EntryMark byte

const (
	Normal EntryMark = iota
	Tombstone
)

// Entry provides key size, value size, key, value.
type Entry struct {
	mark  EntryMark
	kLen  uint32
	vLen  uint32
	key   []byte
	value []byte
}

func NewEntry(key, val []byte, mark EntryMark) *Entry {
	e := &Entry{
		mark:  mark,
		kLen:  uint32(len(key)),
		vLen:  uint32(len(val)),
		key:   key,
		value: val,
	}
	return e
}

// Size returns the size of the bytes occupied.
func (e *Entry) Size() uint32 {
	return entryHeaderSize + e.kLen + e.vLen
}

// logOffset is used in keyDir
type logOffset struct {
	fid    uint32
	offset uint32
}

// Index is used in hint file.
type Index struct {
	fid    uint32
	offset uint32
	kLen   uint32
	key    []byte
}

// Size returns the size of the bytes occupied.
func (idx *Index) Size() uint32 {
	return indexHeaderSize + idx.kLen
}
