package core

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/zeebo/xxh3"
	"io"
	"log"
	"os"
)

type segment struct {
	id   int
	file *os.File // open file handle for reading and writing records
	size int64    // size of the segment file in bytes
}

func newSegment(dir string, id int) (*segment, error) {
	path := getSegmentPath(dir, id)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create segment file %q: %w", path, err)
	}

	return &segment{id: id, file: f, size: 0}, nil
}

func parseSegment(dir string, id int, verifyChecksum bool) (rseg *segment, recs []*scannedRecord, rerr error) {
	path := getSegmentPath(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open segment file %q: %w", path, err)
	}

	seg := &segment{id: id, file: f}

	defer func() {
		if rerr != nil {
			if err := seg.file.Close(); err != nil {
				log.Printf("close segment %d: %v", seg.id, err)
			}
		}
	}()

	// collect the records from the current segment
	rs := newRecordScanner(seg, verifyChecksum)
	for rs.scan() {
		recs = append(recs, rs.record)
	}

	if err := rs.err; err != nil {
		return nil, nil, fmt.Errorf("scan segment %d: %w", seg.id, err)
	}

	// update segment size with the last correct offset
	seg.size = rs.end

	// in case where we have a corrupted record,
	// we truncate to the last "good" offset
	if err := seg.file.Truncate(seg.size); err != nil {
		return nil, nil, fmt.Errorf("truncate segment %d: %w", seg.id, err)
	}

	// Go to the "new" end of the file in case it's truncated
	if _, err := seg.file.Seek(0, io.SeekEnd); err != nil {
		return nil, nil, fmt.Errorf("seek on truncated segment %d: %w", seg.id, err)
	}

	return seg, recs, nil
}

// write writes record to the segment and returns the key offset
func (s *segment) write(key string, val string, wt WriteType, fsync bool) (int64, error) {
	off := s.size

	n, err := writeKV(s.file, wt, key, val)
	if err != nil {
		return 0, fmt.Errorf("writeKV on segment %d: %w", s.id, err)
	}

	// increase file size by the written byte count
	s.size += n

	if fsync {
		// I can use fsync if I want fsync‐per‐write durability
		// fsync is crazy, it costs like 5ms. We could only accept this
		// in group commit scenario.
		if err := s.file.Sync(); err != nil {
			return 0, fmt.Errorf("sync segment %d: %w", s.id, err)
		}
	}

	return off, nil
}

func (s *segment) read(off int64, verifyChecksum bool) (string, WriteType, error) {
	return readKV(s.file, off, verifyChecksum)
}

type WriteType int8

const (
	TypeDelete WriteType = iota
	TypeSet
)

const hdrLen = 18 // 8B checksum + 4B keyLen + 4B valLen + 1 writeType + 1 reserved

// todo think about using crc32c, it's 4B instead of 8
const csLen = 8

// writeKV emits a record of:
//
//	[8-byte checksum][4-byte keyLen][4-byte valLen][1-byte writeType][1-byte reserved][key bytes][val bytes]
//
// and returns the total length
func writeKV(w io.Writer, wt WriteType, key string, val string) (int64, error) {
	// Build complete record in memory for single write
	totalLen := hdrLen + len(key) + len(val)
	buf := make([]byte, totalLen)

	sb := buf // shrinking buffer

	// skipping checksum(buf[:csLen]), we will calculate it last
	sb = sb[csLen:]

	binary.LittleEndian.PutUint32(sb, uint32(len(key)))
	sb = sb[4:]

	binary.LittleEndian.PutUint32(sb, uint32(len(val)))
	sb = sb[4:]

	sb[0] = byte(wt)
	sb = sb[1:]

	sb[0] = 0 // reserved. exists just to make header length even.
	sb = sb[1:]

	// Copy key and value
	copy(sb, key)
	sb = sb[len(key):]

	copy(sb, val)
	sb = sb[len(val):]

	if len(sb) != 0 {
		log.Panicf("unexpected remaining data on buffer: %v", sb)
	}

	// now create the checksum
	checksum := xxh3.Hash(buf[csLen:])
	binary.LittleEndian.PutUint64(buf[:csLen], checksum)

	// Write the buffer in a single syscall
	_, err := w.Write(buf)
	return int64(totalLen), err
}

// readKV reads back a single record at offset in two syscalls:
//  1. ReadAt 18 bytes → header[0:8]=checksum, header[8:12]=keyLen, header[12:16]=valLen, header[16]=writeType, header[17] reserved
//  2. ReadAt keyLen+valLen bytes → payload
//
// I'm okay with two syscalls, no need to optimize them
// because they don't lead to two disk reads thanks to page cache
func readKV(r io.ReaderAt, off int64, verifyChecksum bool) (string, WriteType, error) {
	var hdr [hdrLen]byte
	if _, err := r.ReadAt(hdr[:], off); err != nil {
		return "", 0, err
	}

	checksum, keyLen, valLen, wt := parseHeader(hdr)

	totalLen := hdrLen + keyLen + valLen
	buf := make([]byte, totalLen)
	copy(buf, hdr[:]) // buf[:hdrLen] filled

	// Read key+val into the remaining part
	if _, err := r.ReadAt(buf[hdrLen:], off+hdrLen); err != nil {
		return "", wt, err
	}

	// on checksum problems on single record reads, we just return the error but db continues to operate.
	if verifyChecksum {
		if computed := xxh3.Hash(buf[csLen:]); checksum != computed {
			return "", wt, fmt.Errorf("%w: expected %x, got %x", ErrChecksumMismatch, checksum,
				computed)
		}
	}

	val := string(buf[hdrLen+keyLen:])
	return val, wt, nil
}

// scannedRecord is used by recordScanner to keep information about current record
type scannedRecord struct {
	key string
	val string
	off int64 // start offset of the record in the segment
	wt  WriteType
}

// recordScanner is a buffered segment reader that doesn't touch file handle
type recordScanner struct {
	reader         *bufio.Reader
	record         *scannedRecord // keeps the current record information
	end            int64          // keeps the end offset of the current record
	err            error          // keeps error state
	verifyChecksum bool
}

func newRecordScanner(s *segment, verifyChecksum bool) *recordScanner {
	const maxint64 = 1<<63 - 1 // maybe check file size instead

	// we're using SectionReader so we don't touch the file handle
	// this way we run scan the file repeatedly
	sr := io.NewSectionReader(s.file, 0, maxint64)
	return &recordScanner{reader: bufio.NewReader(sr), verifyChecksum: verifyChecksum}
}

func parseHeader(hdr [hdrLen]byte) (uint64, int, int, WriteType) {
	sb := hdr[:] // shrinking buffer

	checksum := binary.LittleEndian.Uint64(sb)
	sb = sb[csLen:]

	keyLen := int(binary.LittleEndian.Uint32(sb))
	sb = sb[4:]

	valLen := int(binary.LittleEndian.Uint32(sb))
	sb = sb[4:]

	wt := WriteType(sb[0])
	sb = sb[1:]

	_ = sb[0] // reserved byte
	sb = sb[1:]

	if len(sb) != 0 {
		log.Panicf("unexpected remaining data on buffer: %v", sb)
	}

	return checksum, keyLen, valLen, wt
}

func (rs *recordScanner) scan() bool {
	// we stop processing further after an error
	if rs.err != nil {
		return false
	}

	reader := rs.reader

	// resetting the record
	rs.record = nil

	// Either EOF
	isEOF := func(err error) bool {
		return err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF)
	}

	var hdr [hdrLen]byte

	// read the header
	if _, err := io.ReadFull(reader, hdr[:]); err != nil {
		if !isEOF(err) {
			rs.err = fmt.Errorf("read key/val length: %w", err)
		}

		// this is the happy path of exiting the loop
		// we should not have EOF after this, that would mean partially
		// written records i.e. corruption
		return false
	}
	checksum, keyLen, valLen, wt := parseHeader(hdr)

	totalLen := hdrLen + keyLen + valLen
	buf := make([]byte, totalLen)
	copy(buf, hdr[:]) // buf[:hdrLen] filled

	// Read key+val into the remaining part
	if _, err := io.ReadFull(reader, buf[hdrLen:]); err != nil {
		if !isEOF(err) {
			rs.err = fmt.Errorf("read key+value: %w", err)
		}

		// EOF here means partially written key/value i.e. corruption
		// we bail out here, we're just ignoring the partially written key/value
		return false
	}

	// notice that above we skip on partial tail records, but we error out on checksum issues
	// the reasoning: mid-segment corruptions are critical because the records affected by them
	// were persisted correctly and acknowledged to the client(especially when fsync enabled).
	// But partial records on tail only mean db closed for some reason(power loss) and client
	// didn't get any acknowledgement. Therefore, we can choose to ignore them.
	if rs.verifyChecksum {
		if computed := xxh3.Hash(buf[csLen:]); checksum != computed {
			rs.err = fmt.Errorf("%w: expected %x, got %x", ErrChecksumMismatch, checksum,
				computed)
			return false
		}
	}

	rs.record = &scannedRecord{
		key: string(buf[hdrLen : hdrLen+keyLen]),
		val: string(buf[hdrLen+keyLen:]),
		off: rs.end,
		wt:  wt,
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
