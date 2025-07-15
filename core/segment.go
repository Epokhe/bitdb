package core

import (
	"fmt"
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
	rs := newRecordScanner(seg.file, verifyChecksum)
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

	n, err := writeRecord(s.file, wt, key, val)
	if err != nil {
		return 0, fmt.Errorf("writeRecord on segment %d: %w", s.id, err)
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
	return readRecord(s.file, off, verifyChecksum)
}
