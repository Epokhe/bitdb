package core

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type segment struct {
	id     int
	file   *os.File      // open file handle for reading records
	size   int64         // size of the segment file in bytes
	writer *bufio.Writer // buffered writer for segment
}

func newSegment(dir string, id int) (*segment, error) {
	path := getSegmentPath(dir, id)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create segment file %q: %w", path, err)
	}

	w := bufio.NewWriter(f)

	return &segment{id: id, file: f, writer: w, size: 0}, nil
}

type keyOffset struct {
	key string
	off int64
}

func parseSegment(dir string, id int) (*segment, []keyOffset, error) {
	path := getSegmentPath(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open segment file %q: %w", path, err)
	}

	seg := &segment{id: id, file: f}

	defer func() {
		if err != nil {
			_ = seg.file.Close()
		}
	}()

	var keyOffs []keyOffset

	// collect the key offsets from the current segment
	rs := newRecordScanner(seg)
	for rs.scan() {
		r := rs.record
		keyOffs = append(keyOffs, keyOffset{key: r.key, off: r.off})
	}

	err = rs.err // catch the possible error from scan

	// update segment size with the last correct offset
	seg.size = rs.end

	// in case where we have a corrupted record,
	// we truncate to the last "good" offset
	err = seg.file.Truncate(seg.size)
	if err != nil {
		return nil, nil, err
	}

	// Go to the "new" end of the file in case it's truncated
	_, err = seg.file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, nil, err
	}

	seg.writer = bufio.NewWriter(f)

	return seg, keyOffs, nil
}

// write writes record to the segment and returns the key offset
func (s *segment) write(key string, val string, wt WriteType, fsync bool) (int64, error) {
	off := s.size

	n, err := writeKV(s.writer, wt, key, val)
	if err != nil {
		return 0, err
	}

	// todo check why we need to keep file size.
	//  If I do flush, is it really needed?
	// increase file size by the written byte count
	s.size += n

	// flush into the OS page cache so ReadAt will see it
	// todo measure the cost
	if err := s.writer.Flush(); err != nil {
		return 0, err
	}

	if fsync {
		// I can use fsync if I want fsync‐per‐write durability
		// fsync is crazy, it costs like 5ms. We could only accept this
		// in group commit scenario.
		if err := s.file.Sync(); err != nil {
			return 0, err
		}
	}

	return off, nil
}

func (s *segment) finalize() error {
	if err := s.writer.Flush(); err != nil {
		return err
	}

	if err := s.file.Sync(); err != nil {
		return err
	}

	return nil
}

type WriteType int8

const (
	TypeDelete WriteType = iota
	TypeSet
)

const hdrLen = 10 // 4B keyLen + 4B valLen + 1 writeType + 1 reserved

// writeKV now emits:
//
//	[4-byte keyLen][4-byte valLen][1-byte writeType][1-byte reserved]  ← one 10-byte write
//	[key bytes]                      ← one write
//	[val bytes]                      ← one write
//
// returns the total length
func writeKV(w *bufio.Writer, wt WriteType, key string, val string) (int64, error) {
	// Build an 10-byte header on the stack
	var hdr [hdrLen]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(val)))
	hdr[8] = byte(wt)
	hdr[9] = 0 // currently not used. exists just to make header 10 bytes.

	// Write header
	if _, err := w.Write(hdr[:]); err != nil {
		return 0, err
	}

	// Write key
	if _, err := w.WriteString(key); err != nil {
		return 0, err
	}

	// Write value
	_, err := w.WriteString(val)

	writeLen := int64(hdrLen + len(key) + len(val))

	return writeLen, err
}

// scannedRecord is used by recordScanner to keep information about current record
type scannedRecord struct {
	key string
	val string
	off int64 // start offset of the record in the segment
}

// recordScanner is a buffered segment reader that doesn't touch file handle
type recordScanner struct {
	reader *bufio.Reader
	record *scannedRecord // keeps the current record information
	end    int64          // keeps the end offset of the current record
	err    error          // keeps error state
}

func newRecordScanner(s *segment) *recordScanner {
	const maxint64 = 1<<63 - 1 // maybe check file size instead
	sr := io.NewSectionReader(s.file, 0, maxint64)
	return &recordScanner{reader: bufio.NewReader(sr)}
}

func (rs *recordScanner) scan() bool {
	// we stop processing further after an error
	if rs.err != nil {
		return false
	}

	reader := rs.reader

	// resetting the record
	rs.record = nil

	// header for key/value length prefixes
	hdr := make([]byte, hdrLen)

	// Either EOF
	isEOF := func(err error) bool {
		return err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF)
	}

	// read the key/value length
	if _, err := io.ReadFull(reader, hdr); err != nil {
		if !isEOF(err) {
			rs.err = fmt.Errorf("read key/val length: %w", err)
		}

		// this is the happy path of exiting the loop
		// we should not have EOF after this, that would mean partially
		// written records i.e. corruption
		return false
	}
	keyLen := int(binary.LittleEndian.Uint32(hdr[0:4]))
	valLen := int(binary.LittleEndian.Uint32(hdr[4:8]))

	// read the key payload
	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, keyBytes); err != nil {
		if !isEOF(err) {
			rs.err = fmt.Errorf("read key: %w", err)
		}

		// EOF here means partially written key i.e. corruption
		// we bail out here, we're just ignoring the partially written key
		return false
	}

	// read the value payload
	valBytes := make([]byte, valLen)
	if _, err := io.ReadFull(reader, valBytes); err != nil {
		if !isEOF(err) {
			rs.err = fmt.Errorf("read value: %w", err)
		}

		// EOF here means partially written value i.e. corruption
		// we bail out here, we're just ignoring the partially written value
		return false
	}

	rs.record = &scannedRecord{
		key: string(keyBytes),
		val: string(valBytes),
		off: rs.end,
	}

	// todo consider making this function configurable so that
	//  it may skip values when only keys are needed.
	//  the best approach may be to give a read/skip choice for
	//  each key separately, because on segment merge we decide
	//  per key to read the value!
	//// skip value payload because we don't need it on the index
	//if _, err := io.CopyN(io.Discard, reader, int64(valLen)); err != nil {
	//	if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
	//		break
	//	}
	//
	//	// EOF here means partially written value i.e. corruption
	//	// we bail out here, we're just ignoring the partially written value
	//	return 0, err
	//}

	// advance offset for next record
	rs.end += int64(hdrLen + keyLen + valLen)

	return true
}
