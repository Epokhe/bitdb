package core

import (
	"fmt"
	"log"
	"os"
)

type segment struct {
	id       int
	file     *os.File // open file handle for reading and writing
	scanFile *os.File // open file handle for sequential scanning. initialized late.
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

	// active/inactive diffs
	// ro/rw file
	// scanfile
	// truncate/seek shit

	// what if we have a uniform parsing
	// ro file
	// put scanfile

	// then we make last segment active by reopen rw
	// dont care about scanfile
	// even on creation side dont have ifs on scanfile
	// just comment scanfile is only expected to be used after RO
	// do the truncate/seek shit on the active segment after RW

	f, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open segment file %q: %w", path, err)
	}

	defer func() {
		if rerr != nil {
			if err := f.Close(); err != nil {
				log.Printf("close segment %d file: %v", id, err)
			}
		}
	}()

	// we have an additional scanFile handle for sequential scanning
	var sf *os.File
	sf, err = os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open segment scan file %q: %w", path, err)
	}

	defer func() {
		if rerr != nil {
			if err := sf.Close(); err != nil {
				log.Printf("close segment %d scan file: %v", id, err)
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

	// segment size is equal to the last successfully scanned offset
	size := rs.end

	seg := &segment{id: id, file: f, scanFile: sf, size: size}
	return seg, recs, nil
}

// we seal a segment by making its file handle read-only
func (s *segment) seal() error {
	path := s.file.Name()

	// todo flush before close?
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("close segment %d file: %w", s.id, err)
	}

	roFile, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open segment file %q: %w", path, err)
	}
	s.file = roFile

	return nil
}

func (s *segment) unseal() error {
	path := s.file.Name()

	if err := s.file.Close(); err != nil {
		return fmt.Errorf("close segment %d file: %w", s.id, err)
	}

	roFile, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open segment file %q: %w", path, err)
	}
	s.file = roFile

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
