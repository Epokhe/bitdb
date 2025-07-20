package core

import (
	"fmt"
	"io"
	"log"
	"os"
)

type segment struct {
	id       int
	file     *os.File // open file handle for reading and writing
	scanFile *os.File // open file handle for sequential scanning
	size     int64    // size of the segment file in bytes
}

func newSegment(dir string, id int) (*segment, error) {
	path := getSegmentPath(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create segment file %q: %w", path, err)
	}

	return &segment{id: id, file: f, size: 0}, nil
}

func parseSegment(dir string, id int, verifyChecksum bool) (rseg *segment, recs []*scannedRecord, rerr error) {
	path := getSegmentPath(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644) // todo readonly for all but last
	if err != nil {
		return nil, nil, fmt.Errorf("open segment file %q: %w", path, err)
	}

	defer func() {
		if rerr != nil {
			if err := f.Close(); err != nil {
				log.Printf("close segment %d: %v", id, err)
			}
		}
	}()

	// collect the records from the current segment
	rs := newRecordScanner(f, verifyChecksum)
	for rs.scan() {
		recs = append(recs, rs.record)
	}

	if err := rs.err; err != nil {
		return nil, nil, fmt.Errorf("scan segment %d: %w", id, err)
	}

	// segment size is equal to the last scanned offset
	size := rs.end

	// in case where we have a corrupted record,
	// we truncate to the last "good" offset
	if err := f.Truncate(size); err != nil {
		return nil, nil, fmt.Errorf("truncate segment %d: %w", id, err)
	}

	// Go to the "new" end of the file in case it's truncated
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return nil, nil, fmt.Errorf("seek on truncated segment %d: %w", id, err)
	}

	seg := &segment{id: id, file: f, size: size}
	return seg, recs, nil
}

// we make a segment inactive by making its file handle read-only
func (s *segment) deactivate() error {
	path := s.file.Name() // i think this'll work?

	if err := s.file.Close(); err != nil {
		return fmt.Errorf("close segment %d file: %w", s.id, err)
	}

	roFile, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open segment file %q: %w", path, err)
	}
	s.file = roFile

	scanFile, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open segment scan file %q: %w", path, err)
	}
	s.scanFile = scanFile

	return nil
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
