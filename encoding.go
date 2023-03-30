package minidb

import (
	"encoding/binary"
	"github.com/pingcap/errors"
)

func encodeEntry(e *Entry) ([]byte, error) {
	buf := make([]byte, e.Size())

	buf[0] = byte(e.mark)
	binary.BigEndian.PutUint32(buf[1:5], e.kLen)
	binary.BigEndian.PutUint32(buf[5:9], e.vLen)
	copy(buf[entryHeaderSize:], e.key)
	copy(buf[entryHeaderSize+e.kLen:], e.value)

	return buf, nil
}

func decodeEntry(buf []byte) (*Entry, error) {
	if len(buf) < entryHeaderSize {
		return nil, errors.Errorf("len(buf) must greater than or equal to %d", entryHeaderSize)
	}
	kLen := binary.BigEndian.Uint32(buf[1:5])
	vLen := binary.BigEndian.Uint32(buf[5:9])

	e := &Entry{
		mark: EntryMark(buf[0]),
		kLen: kLen,
		vLen: vLen,
	}
	if len(buf) > entryHeaderSize {
		key := make([]byte, kLen)
		value := make([]byte, vLen)
		copy(key, buf[entryHeaderSize:entryHeaderSize+kLen])
		copy(value, buf[entryHeaderSize+kLen:])
		e.key = key
		e.value = value
	}
	return e, nil
}

func encodeIndex(idx *Index) ([]byte, error) {
	buf := make([]byte, idx.Size())
	binary.BigEndian.PutUint32(buf[:4], idx.fid)
	binary.BigEndian.PutUint32(buf[4:8], idx.offset)
	binary.BigEndian.PutUint32(buf[8:12], idx.kLen)
	copy(buf[indexHeaderSize:], idx.key)
	return buf, nil
}

func decodeIndex(buf []byte) (*Index, error) {
	idx := &Index{
		fid:    binary.BigEndian.Uint32(buf[:4]),
		offset: binary.BigEndian.Uint32(buf[4:8]),
		kLen:   binary.BigEndian.Uint32(buf[8:12]),
	}
	return idx, nil
}
