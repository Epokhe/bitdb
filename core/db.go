package core

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type DB struct {
	dir      string     // data directory
	segments []*segment // all segments. last one is the active segment
	//writer         *bufio.Writer              // buffered writer for the currently active segment
	segmentSizeMax int64                      // maximum size a segment can reach
	fsync          bool                       // whether to fsync on each Set call
	sem            chan struct{}              // merge semaphore
	mu             sync.Mutex                 // id counter mutex
	idCtr          int                        // segment id counter, guarded by mu
	index          map[string]*recordLocation // maps each key to its last-seen location
	manifest       *os.File                   // open file handle for manifest
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
		sem:            make(chan struct{}, 1),
		index:          make(map[string]*recordLocation),
	}

	// apply options
	for _, opt := range opts {
		opt(db)
	}

	// DO NOT SHADOW err so defer does not miss it
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
	var maxId int
	scanner := bufio.NewScanner(db.manifest)
	for scanner.Scan() {
		var segId int
		segId, _ = strconv.Atoi(scanner.Text())

		var seg *segment
		var kOffs []keyOffset
		seg, kOffs, err = parseSegment(db.dir, segId)
		if err != nil {
			return nil, fmt.Errorf("loadsegment %q: %w", segId, err)
		}

		// update db index with the returned offsets
		for _, kOff := range kOffs {
			db.index[kOff.key] = &recordLocation{seg: seg, offset: kOff.off}
		}

		db.activateSegment(seg)

		// also, compute max segment id so we can set the counter
		if segId > maxId {
			maxId = segId
		}
	}

	if err = scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	// set the segment id counter
	db.idCtr = maxId + 1

	// in case this is a new folder, we create the empty segment
	if len(db.segments) == 0 {
		// log.Println("No segment found, creating a new one...")
		if err = db.createSegment(); err != nil {
			return nil, fmt.Errorf("createsegment: %w", err)
		}
	}

	return db, nil
}

// todo remove this and add new segment info to createSegment function
func (db *DB) activeSegment() *segment {
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
		mnf, err := createFileDurable(dir, "MANIFEST")
		if err != nil {
			return nil, fmt.Errorf("create manifest: %q", err)
		}

		return mnf, nil
	} else {
		// manifest already exists, return it
		mnf, err := os.OpenFile(manifestPath, os.O_RDWR, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open manifest: %w", err)
		}
		return mnf, nil
	}
}

func (db *DB) overwriteManifest() error {
	var buf bytes.Buffer

	for _, seg := range db.segments {
		fmt.Fprintf(&buf, "%d\n", seg.id)
	}

	if newf, err := writeFileAtomic(db.manifest, buf.Bytes()); err != nil {
		return fmt.Errorf("atomic write manifest: %w", err)
	} else {
		db.manifest = newf
	}

	return nil
}

func getSegmentPath(dir string, id int) string {
	return filepath.Join(dir, fmt.Sprintf("seg%03d", id))
}

func (db *DB) claimNextSegmentId() int {
	db.mu.Lock()
	nextId := db.idCtr
	db.idCtr += 1
	db.mu.Unlock()
	return nextId
}

// creates an empty segment and appends it to the segment list.
// Changes the writer so new data is written to this segment.
func (db *DB) createSegment() error {
	id := db.claimNextSegmentId()

	seg, err := newSegment(db.dir, id)
	if err != nil {
		return fmt.Errorf("new segment %q: %w", id, err)
	}

	db.activateSegment(seg)
	if err := db.overwriteManifest(); err != nil {
		return fmt.Errorf("overwrite manifest: %w", err)
	}

	return nil
}

// adds the segment to the list which activates it
func (db *DB) activateSegment(seg *segment) {
	db.segments = append(db.segments, seg)
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

// recordLocation keeps the address of a record in the multi-segment data layout
type recordLocation struct {
	seg    *segment
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
		return "", fmt.Errorf("db.readValueAt recordLocation%+v: %w", loc, err)
	}

	return val, nil
}

// readValueAt reads back a single record at offset in two syscalls:
//  1. ReadAt 8 bytes → header[0:4]==keyLen, header[4:8]==valLen
//  2. ReadAt keyLen+valLen bytes → payload
//
// I'm okay with two syscalls, no need to optimize them
// because they don't lead to two disk reads thanks to page cache
func (db *DB) readValueAt(loc *recordLocation) (val string, err error) {
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

func (db *DB) tryMerge() {
	select {
	case db.sem <- struct{}{}:
		go func() {
			defer func() {
				// release when done
				<-db.sem
			}()
			fmt.Println("goroutine started")
			time.Sleep(3 * time.Second)

			if err := db.merge(); err != nil {
				return err
			}

			fmt.Println("goroutine finished")
		}()

	default:
		fmt.Println("already running sry")
	}
}

func (db *DB) Set(key, val string) error {
	//db.tryMerge()
	if db.activeSegment().size > db.segmentSizeMax {
		// we will close the current segment and create a new segment here.
		// Since I'm already flushing on every set, I can just re-assign the writer here.
		if err := db.createSegment(); err != nil {
			return err
		}

		if len(db.segments) > 100 { // this is for now, there will be a better way to decide later

			//db.tryMerge()
		}
	}

	seg := db.activeSegment()

	off, err := seg.write(key, val, db.fsync)
	if err != nil {
		return err
	}

	// add current key's location to index
	// offset equals size since we're appending to the file
	// if power is lost just before this line, no prob,
	// index will be rebuilt anyway
	db.index[key] = &recordLocation{seg: seg, offset: off}

	return nil
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
