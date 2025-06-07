package core

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

const DefaultSegmentSizeMax int64 = 1 * 1024 * 1024

type Segment struct {
	id   int
	path string   // path to the segment file
	file *os.File // open file handle for reading records
	size int64    // size of the segment file in bytes
}

type DB struct {
	dir            string               // data directory
	segments       []*Segment           // all segments. last one is the active segment
	writer         *bufio.Writer        // buffered writer for the currently active segment
	segmentSizeMax int64                // maximum size a segment can reach
	fsync          bool                 // whether to fsync on each Set call
	index          map[string]*Location // maps each key to its last-seen Location
	manifest       *os.File             // open file handle for manifest
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
	db := &DB{
		dir:            dir,
		segmentSizeMax: DefaultSegmentSizeMax,
		fsync:          false,
		index:          make(map[string]*Location),
	}

	// apply options
	for _, opt := range opts {
		opt(db)
	}

	// err in this function should not be redeclared, if not
	// the defer below will miss them
	var err error

	// if we're erroring out, run abort process
	defer func() {
		if err != nil {
			db.AbortOnOpen()
		}
	}()

	if err = os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %q: %w", dir, err)
	}

	db.manifest, err = ensureManifest(db.dir)
	if err != nil {
		return nil, fmt.Errorf("ensuremanifest: %w", err)
	}

	// load all segments according to manifest file
	scanner := bufio.NewScanner(db.manifest)
	for scanner.Scan() {
		var segId int
		segId, err = strconv.Atoi(scanner.Text())
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		var seg *Segment
		seg, err = loadSegment(segId, db)
		if err != nil {
			return nil, fmt.Errorf("loadsegment %q: %w", segId, err)
		}

		db.activateSegment(seg)
	}

	// in case this is a new folder, we create the empty segment
	if len(db.segments) == 0 {
		// log.Println("No segment found, creating a new one...")
		if err = db.createSegment(); err != nil {
			return nil, fmt.Errorf("createsegment: %w", err)
		}
	}

	return db, nil
}

func (db *DB) LastSegment() *Segment {
	return db.segments[len(db.segments)-1]
}

func ensureManifest(dir string) (*os.File, error) {
	manifestPath := filepath.Join(dir, "MANIFEST")

	_, err := os.Stat(manifestPath)
	if err != nil && !os.IsNotExist(err) {
		// Some other error trying to Stat
		return nil, fmt.Errorf("stat manifest: %w", err)
	}

	if os.IsNotExist(err) {
		// No manifest, let's create it
		mnf, err := os.OpenFile(manifestPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("create manifest: %w", err)
		}

		// fsync the file
		if err := mnf.Sync(); err != nil {
			return nil, fmt.Errorf("fsync new manifest: %w", err)
		}

		// Fsync the directory so that “the directory entry for MANIFEST”
		// is also committed to disk
		dfd, err := os.Open(dir)
		if err != nil {
			return nil, fmt.Errorf("open parent dir %q: %w", dir, err)
		}

		defer dfd.Close()

		if err := dfd.Sync(); err != nil {
			return nil, fmt.Errorf("fsync parent dir %q: %w", dir, err)
		}

		// Now manifest definitely exists on disk and survives a crash.
		return mnf, nil
	} else {
		// manifest already exists, return it
		mnf, err := os.OpenFile(manifestPath, os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open manifest: %w", err)
		}
		return mnf, nil
	}
}

func (db *DB) appendToManifest(segId int) error {
	// Write the new segment id
	if _, err := fmt.Fprintf(db.manifest, "%d\n", segId); err != nil {
		return fmt.Errorf("write to manifest: %w", err)
	}

	// Fsync to disk
	if err := db.manifest.Sync(); err != nil {
		return fmt.Errorf("fsync manifest: %w", err)
	}

	return nil
}

func getSegmentPath(dir string, id int) string {
	return filepath.Join(dir, fmt.Sprintf("seg%03d", id))
}

func (db *DB) nextSegmentId() int {
	if len(db.segments) == 0 {
		return 1
	} else {
		return db.LastSegment().id + 1 // increment id
	}
}

// creates an empty segment and appends it to the segment list.
// Changes the writer so new data is written to this segment.
func (db *DB) createSegment() error {
	id := db.nextSegmentId()
	path := getSegmentPath(db.dir, id)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("createsegment %q: %w", id, err)
	}

	// create an empty segment with the new file
	seg := &Segment{id: id, path: path, file: f, size: 0}
	db.activateSegment(seg)
	if err := db.appendToManifest(seg.id); err != nil {
		return fmt.Errorf("appendtomanifest %q: %w", id, err)
	}

	return nil
}

// adds the segment to the list and activates it
// by assigning a new writer for it
func (db *DB) activateSegment(seg *Segment) {
	db.segments = append(db.segments, seg)
	// in the current state, writer.flush is called on each Set call,
	// so I'm not calling flush for the old writer
	//db.writer.Flush()
	db.writer = bufio.NewWriter(seg.file)
}

func loadSegment(id int, db *DB) (*Segment, error) {
	path := getSegmentPath(db.dir, id)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	seg := Segment{id: id, path: path, file: f}

	reader := bufio.NewReader(f)
	offset, err := fillIndex(reader, &seg, db.index)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	seg.size = offset

	// in case where we have a corrupted record,
	// we truncate to the last "good" offset
	if err := f.Truncate(offset); err != nil {
		return nil, err
	}

	// Go to the "new" end of the file in case it's truncated
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return &seg, nil
}

func (db *DB) Close() error {
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

	// close the manifest
	_ = db.manifest.Close()

	return nil
}

// AbortOnOpen In case a failure happens during Open,
// we need to clean-up stuff opened so far. Keeping this
// separate from Close, which ensures graceful shutdown.
func (db *DB) AbortOnOpen() {
	// close all segments which are opened so far
	for _, s := range db.segments {
		_ = s.file.Close()
	}

	// close the manifest if it was opened
	if db.manifest != nil {
		_ = db.manifest.Close()
	}
}

// fills the db index with the key locations from the current segment
func fillIndex(reader *bufio.Reader, seg *Segment, index map[string]*Location) (int64, error) {
	var offset int64 = 0

	// header for key/value length prefixes
	hdr := make([]byte, 8)

	for {
		// read the key/value length
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

		// record the Location for this key
		index[key] = &Location{
			seg:    seg,
			offset: offset,
		}

		// advance offset for next record
		offset += int64(8 + keyLen + valLen)

	}

	return offset, nil
}

// Location keeps the address of a record in the multi-segment data layout
type Location struct {
	seg    *Segment
	offset int64
}

func (db *DB) Get(key string) (string, error) {
	loc, ok := db.index[key]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}

	val, err := db.readValueAt(loc)
	if err != nil {
		// this is an unexpected error, because if key is on index,
		// its corresponding value should exist on the disk file
		return "", fmt.Errorf("db.readValueAt Location%+v: %w", loc, err)
	}

	return val, nil
}

// readValueAt reads back a single record at offset in two syscalls:
//  1. ReadAt 8 bytes → header[0:4]==keyLen, header[4:8]==valLen
//  2. ReadAt keyLen+valLen bytes → payload
//
// I'm okay with two syscalls, no need to optimize them
// because they don't lead to two disk reads thanks to page cache
func (db *DB) readValueAt(loc *Location) (val string, err error) {
	// Read both lengths at once
	var hdr [8]byte
	file := loc.seg.file
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
	if db.LastSegment().size > db.segmentSizeMax {
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

	// add current key's Location to index
	// offset equals size since we're appending to the file
	// if power is lost just before this line, no prob,
	// index will be rebuilt anyway
	ls := db.LastSegment()
	db.index[key] = &Location{seg: ls, offset: ls.size}

	// todo check why we need to keep file size. If I do flush, is it really needed?
	// increase file size by the written byte count
	ls.size += int64(8 + len(key) + len(val)) // this calculation may be moved to writeKV

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
