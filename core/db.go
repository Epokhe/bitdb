package core

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
)

const DefaultSegmentSizeMax int64 = 1 * 1024 * 1024

type Segment struct {
	path  string           // path to the segment file
	file  *os.File         // open file handle for reading records
	index map[string]int64 // maps each key to its last-seen offset in the segment
	size  int64            // size of the segment file in bytes
}

type DB struct {
	dir            string        // data directory
	segments       []*Segment    // all segments. last one is the active segment
	writer         *bufio.Writer // buffered writer for the currently active segment
	segmentSizeMax int64         // maximum size a segment can reach
	fsync          bool          // whether to fsync on each Set call
}

var ErrKeyNotFound = errors.New("key not found")

func WithSegmentSizeMax(n int64) Option {
	return func(db *DB) { db.segmentSizeMax = n }
}

func WithFsync(b bool) Option {
	return func(db *DB) { db.fsync = b }
}

type Option func(*DB)

func Open(dir string, opts ...Option) (*DB, error) {
	db := &DB{dir: dir, segmentSizeMax: DefaultSegmentSizeMax, fsync: false}

	// apply options
	for _, opt := range opts {
		opt(db)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %q: %w", dir, err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("readdir %q: %w", dir, err)
	}

	defer func() {
		if err != nil {
			// if we're erroring out, let's close all the opened files
			for _, s := range db.segments {
				s.file.Close()
			}
		}
	}()

	// load all segments
	for _, e := range entries {
		path := filepath.Join(dir, e.Name())

		seg, err := loadSegment(path)
		if err != nil {
			return nil, fmt.Errorf("loadsegment %q: %w", path, err)
		}

		db.addSegment(seg)
	}

	// if last segment gets filled, what do we do?
	// I think at that point(in the same thread), we create a new segment. Then return the result of set.
	// This ensures that if there's a segment file, the last one is always an open writable segment.
	// This way, we just check if a segment exists for initial creation.

	// in case this is a new folder, we create the empty segment
	if len(db.segments) == 0 {
		// log.Println("No segment found, creating a new one...")
		if err := db.createSegment(); err != nil {
			return nil, fmt.Errorf("createsegment: %w", err)
		}
	}

	return db, nil
}

func (db *DB) LastSegment() *Segment {
	return db.segments[len(db.segments)-1]
}

// creates an empty segment and appends it to the segment list.
// Changes the writer so new data is written to this segment.
func (db *DB) createSegment() error {
	path := filepath.Join(db.dir, fmt.Sprintf("seg%03d", len(db.segments)+1))
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("createsegment %q: %w", path, err)
	}

	// create an empty segment with the new file
	seg := &Segment{path: path, file: f, index: make(map[string]int64), size: 0}
	db.addSegment(seg)
	return nil
}

// we add segments via this function because each segment addition
// requires changing the writer too.
func (db *DB) addSegment(seg *Segment) {
	db.segments = append(db.segments, seg)
	// in the current state, writer.flush is called on each Set call,
	// so I'm not calling flush for the old writer
	//db.writer.Flush()
	db.writer = bufio.NewWriter(seg.file)
}

func loadSegment(path string) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(f)
	index := make(map[string]int64)
	offset, err := loadIndex(reader, index)
	if err != nil {
		f.Close()
		return nil, err
	}

	// in case where we have a corrupted record,
	// we truncate to the last "good" offset
	if err := f.Truncate(offset); err != nil {
		return nil, err
	}

	// Go to the "new" end of the file in case it's truncated
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return &Segment{path: path, file: f, index: index, size: offset}, nil
}

func (db *DB) Close() error {
	// flush buffered bytes into the OS page cache
	// yesss, on power loss we lose these
	// ignored for now
	if err := db.writer.Flush(); err != nil {
		return err
	}

	// close all segments
	for _, s := range db.segments {
		// block until the OS has flushed those pages to stable storage
		if err := s.file.Sync(); err != nil {
			return err
		}

		// close the file
		if err := s.file.Close(); err != nil {
			return err
		}
	}

	return nil
}

func loadIndex(reader *bufio.Reader, index map[string]int64) (int64, error) {
	var offset int64 = 0

	// header for key/value length prefixes
	hdr := make([]byte, 8)

	for {
		// read the key length
		if _, err := io.ReadFull(reader, hdr); err != nil {
			// this is the happy path of exiting the loop
			// we should never have EOF after this, that would mean partially
			// written records i.e. corruption
			if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return 0, err
		}
		keyLen := int(binary.LittleEndian.Uint32(hdr[0:4]))
		valLen := int(binary.LittleEndian.Uint32(hdr[4:8]))

		// read the key payload
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			// EOF here means partially written key i.e. corruption
			// we bail out here, we're just ignoring the partially written key
			if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}

			return 0, err
		}
		key := string(keyBytes)

		// skip value payload because we don't need it on the index
		if _, err := io.CopyN(io.Discard, reader, int64(valLen)); err != nil {
			// EOF here means partially written value i.e. corruption
			// we bail out here, we're just ignoring the partially written value
			if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return 0, err
		}

		// record the offset for this key
		index[key] = offset

		// advance offset for next record
		offset += int64(8 + keyLen + valLen)

	}

	return offset, nil
}

// Location keeps the address of a record in the multi-segment data layout
type Location struct {
	segIdx int
	offset int64
}

// search each segment for the key and if exists, return its Location
func segmentSearch(db *DB, key string) (loc *Location, err error) {
	found := false

	// we will check each segment's index for the key, starting from the last one
	for i, s := range slices.Backward(db.segments) {
		off, ok := s.index[key]
		if ok {
			found = true
			loc = &Location{segIdx: i, offset: off}
			break
		}
	}

	if found {
		return loc, nil
	} else {
		// if not on any index, the key doesn't exist
		return nil, fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}
}

func (db *DB) Get(key string) (string, error) {
	loc, err := segmentSearch(db, key)
	if err != nil {
		return "", err
	}

	val, err := db.readValueAt(loc)
	if err != nil {
		// this is an unexpected error, because if key is on index,
		// its corresponding value should exist on the disk file
		return "", fmt.Errorf("db.readValueAt Location%v: %w", loc, err)
	}

	return val, nil
}

// readValueAt reads back a single record at offset `off` in two syscalls:
//  1. ReadAt 8 bytes → header[0:4]==keyLen, header[4:8]==valLen
//  2. ReadAt keyLen+valLen bytes → payload
//
// I'm okay with two syscalls, no need to optimize them
// because they don't lead to two disk reads thanks to page cache
func (db *DB) readValueAt(loc *Location) (val string, err error) {
	// Read both lengths at once
	var hdr [8]byte
	file := db.segments[loc.segIdx].file
	if _, err = file.ReadAt(hdr[:], loc.offset); err != nil {
		return "", err
	}
	keyLen := int(binary.LittleEndian.Uint32(hdr[0:4]))
	valLen := int(binary.LittleEndian.Uint32(hdr[4:8]))

	// Read key+val in one go
	buf := make([]byte, valLen)
	if _, err = file.ReadAt(buf, loc.offset+8+int64(keyLen)); err != nil {
		return "", err
	}

	val = string(buf)
	return val, nil
}

func (db *DB) Set(key, val string) error {
	writeLen := int64(8 + len(key) + len(val))

	if db.LastSegment().size+writeLen > db.segmentSizeMax {
		// we will close the current segment and create a new segment here.
		// Since I'm already flushing on every set, I can just re-assign the writer here.
		if err := db.createSegment(); err != nil {
			return err
		}
	}

	// write key-value with length-prefix
	if err := writeKV(db.writer, key, val); err != nil {
		return err
	}

	// flush into the OS page cache so ReadAt will see it
	// todo measure the cost
	if err := db.writer.Flush(); err != nil {
		return err
	}

	if db.fsync {
		// I can use fsync if I want fsync‐per‐write durability
		// fsync is crazy, it costs like 5ms. We could only accept this
		// in group commit scenario.
		if err := db.LastSegment().file.Sync(); err != nil {
			return err
		}
	}

	// add current key's offset to index
	// offset equals size since we're appending to the file
	// if power is lost just before this line, no prob,
	// index will be rebuilt anyway
	ls := db.LastSegment()
	ls.index[key] = ls.size

	// todo check why we need to keep file size. If I do flush, is it really needed?
	// increase file size by the written byte count
	ls.size += writeLen

	return nil
}

// writeKV now emits:
//
//	[4-byte keyLen][4-byte valLen]  ← one 8-byte write
//	[key bytes]                      ← one write
//	[val bytes]                      ← one write
//
// returns the total length
func writeKV(w *bufio.Writer, key, val string) (err error) {
	// Build an 8-byte header on the stack
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(val)))

	// Write header
	_, err = w.Write(hdr[:])
	if err != nil {
		return err
	}

	// Write key
	_, err = w.WriteString(key)
	if err != nil {
		return err
	}

	// Write value
	_, err = w.WriteString(val)

	return err
}

// DiskSize returns the sum of all on-disk segment file sizes.
func (db *DB) DiskSize() (int64, error) {
	var total int64
	for _, seg := range db.segments {
		info, err := seg.file.Stat()
		if err != nil {
			return 0, fmt.Errorf("stat segment file: %w", err)
		}
		total += info.Size()
	}
	return total, nil
}
