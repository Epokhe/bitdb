// Package core provides the core BitDB implementation.
package core

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/deckarep/golang-set/v2"
)

// todo merge configuration under one struct

type DB struct {
	dir               string                     // data directory
	segments          []*segment                 // all segments. last one is the active segment
	fsync             bool                       // whether to fsync on each Set call
	mergeSem          chan struct{}              // merge semaphore
	rw                sync.RWMutex               // guards segments & index & manifest
	mergeErr          chan error                 // async merge error reporting
	idCtr             int64                      // segment id counter
	index             map[string]*recordLocation // maps each key to its last-seen location
	manifest          *os.File                   // open file handle for manifest
	mergeEnabled      bool                       // whether merge is enabled
	rolloverThreshold int64                      // rollover segment when the active segment reaches this
	mergeThreshold    int                        // run merge when inactive(merge-able) segment count reaches this
	checksumEnabled   bool                       // enable corruption checks on Open and Get
	onMergeStart      func()                     // test hook
	onMergeApply      func()                     // test hook
}

var ErrKeyNotFound = errors.New("key not found")
var ErrChecksumMismatch = errors.New("checksum mismatch")

func WithRolloverThreshold(n int64) Option {
	return func(db *DB) { db.rolloverThreshold = n }
}

func WithFsync(b bool) Option {
	return func(db *DB) { db.fsync = b }
}

func WithMergeEnabled(b bool) Option {
	return func(db *DB) { db.mergeEnabled = b }
}

func WithMergeThreshold(n int) Option {
	return func(db *DB) {
		db.mergeThreshold = n
	}
}

func WithOnMergeStart(f func()) Option {
	return func(db *DB) {
		db.onMergeStart = f
	}
}

func WithOnMergeApply(f func()) Option {
	return func(db *DB) {
		db.onMergeApply = f
	}
}

func WithChecksumEnabled(b bool) Option {
	return func(db *DB) { db.checksumEnabled = b }
}

type Option func(*DB)

func Open(dir string, opts ...Option) (rdb *DB, rerr error) {
	db := &DB{
		dir:      dir,
		mergeSem: make(chan struct{}, 1),
		index:    make(map[string]*recordLocation),
		// todo mergeErr may not be listened, which will hang the merge goroutine
		//  should i enforce the listen somehow, or drop errors?
		mergeErr:     make(chan error, 1),
		onMergeStart: func() {},
		onMergeApply: func() {},
		// default values
		fsync:             false,
		rolloverThreshold: 1 * 1024 * 1024,
		mergeEnabled:      true,
		mergeThreshold:    100,
		checksumEnabled:   true,
	}

	// apply options
	for _, opt := range opts {
		opt(db)
	}

	// if we're erroring out, run abort process
	defer func() {
		if rerr != nil {
			db.AbortOnOpen()
		}
	}()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %q: %w", dir, err)
	}

	mnf, err := ensureManifest(db.dir)
	if err != nil {
		return nil, fmt.Errorf("ensuremanifest: %w", err)
	}
	db.manifest = mnf

	// we will load the segments ordered by the manifest file
	mnfBytes, err := io.ReadAll(db.manifest)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	// parse the manifest and get segment ids
	mnfIds := strings.Fields(string(mnfBytes))
	var segIds []int
	for _, idStr := range mnfIds {
		id, _ := strconv.Atoi(idStr) // don't expect an error
		segIds = append(segIds, id)
	}

	// load all segments according to parsed manifest
	for _, id := range segIds {
		seg, recs, err := parseSegment(db.dir, id, db.checksumEnabled)
		if err != nil {
			return nil, fmt.Errorf("loadsegment %q: %w", id, err)
		}

		// update db index with the returned records
		// We simulate the history. Sets update the index, deletes remove from the index.
		for _, rec := range recs {
			switch rec.wt {
			case TypeDelete:
				delete(db.index, rec.key)
			case TypeSet:
				db.index[rec.key] = &recordLocation{seg: seg, offset: rec.off}
			default:
				log.Panicf("unhandled write type: %v", rec.wt)
			}
		}

		db.segments = append(db.segments, seg)
	}

	// set the segment id counter
	maxId := 0
	if len(segIds) > 0 {
		maxId = slices.Max(segIds)
	}
	db.idCtr = int64(maxId + 1)

	if err = db.checkOrphanedSegments(segIds); err != nil {
		return nil, fmt.Errorf("cleanup orphaned segments: %w", err)
	}

	// in case this is a new folder, we create the empty segment
	if len(db.segments) == 0 {
		// log.Println("No segment found, creating a new one...")
		if err = db.addSegment(); err != nil {
			return nil, fmt.Errorf("createsegment: %w", err)
		}
	}

	return db, nil
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
	// We atomically increment and return the previous value so callers always
	// get a unique id even under concurrency without needing external locks.
	return int(atomic.AddInt64(&db.idCtr, 1) - 1)
}

// creates an empty segment and appends it to the segment list.
// Changes the writer so new data is written to this segment.
func (db *DB) addSegment() error {
	seg, err := newSegment(db.dir, db.claimNextSegmentId())
	if err != nil {
		return fmt.Errorf("create new segment: %w", err)
	}

	db.segments = append(db.segments, seg)

	if err := db.overwriteManifest(); err != nil {
		return fmt.Errorf("overwrite manifest: %w", err)
	}

	return nil
}

func (db *DB) Close() error {
	db.rw.Lock()
	defer db.rw.Unlock()

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
	db.rw.RLock()
	defer db.rw.RUnlock()

	loc, ok := db.index[key]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}

	val, wt, err := loc.seg.read(loc.offset, db.checksumEnabled)
	if err != nil {
		// this is an unexpected error, because in normal operation,
		// if key is on index, its corresponding value should exist on the disk file
		// this implies possible file corruption
		return "", fmt.Errorf("seg.read recordLocation%+v: %w", loc, err)
	}

	if wt == TypeDelete {
		// todo we should work on preventing this to happen.
		//  stop the db, make it read only etc. this also has a
		//  messy interaction with the merge handling of deleted keys

		// We shouldn't get here in normal circumstances because we're
		// removing the key from db.index on DB.Delete()
		// The only case I can think of is if seg.write() in DB.Delete()
		// returns an error but file write succeeds internally.
		// In that case, db.index won't be deleted so we will enter here
		return "", fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}

	return val, nil
}

func (db *DB) checkRolloverAndMerge(seg *segment) error {
	if seg.size < db.rolloverThreshold {
		return nil
	}

	// we will have a new segment active
	err := db.addSegment()
	if err != nil {
		return err
	}

	// +1 because threshold logic checks only inactive segments
	if db.mergeEnabled && len(db.segments) >= db.mergeThreshold+1 {
		db.tryMerge()
	}

	return nil
}

func (db *DB) Set(key, val string) error {
	db.rw.Lock()
	defer db.rw.Unlock()

	// get active segment
	seg := db.segments[len(db.segments)-1]

	off, err := seg.write(key, val, TypeSet, db.fsync)
	if err != nil {
		return err
	}

	// add current key's location to index
	// offset equals size since we're appending to the file
	// if power is lost just before this line, no prob,
	// index will be rebuilt anyway
	db.index[key] = &recordLocation{seg: seg, offset: off}

	if err = db.checkRolloverAndMerge(seg); err != nil {
		return err
	}

	return nil
}

func (db *DB) Delete(key string) error {
	db.rw.Lock()
	defer db.rw.Unlock()

	_, ok := db.index[key]
	if !ok {
		return fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}

	// get active segment
	seg := db.segments[len(db.segments)-1]

	if _, err := seg.write(key, "", TypeDelete, db.fsync); err != nil {
		return err
	}

	// delete the key. this makes get calls on deleted keys more efficient
	delete(db.index, key)

	if err := db.checkRolloverAndMerge(seg); err != nil {
		return err
	}

	return nil
}

// DiskSize returns the sum of all on-disk segment file sizes.
func (db *DB) DiskSize() (int64, error) {
	db.rw.RLock()
	defer db.rw.RUnlock()

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

// We check orphaned segments in case a power loss occurred during a merge operation
func (db *DB) checkOrphanedSegments(segIds []int) error {
	// scan directory for segment files
	entries, err := os.ReadDir(db.dir)
	if err != nil {
		return fmt.Errorf("read dir: %w", err)
	}

	// segment ids in the manifest
	expected := mapset.NewSet[string]()
	for _, id := range segIds {
		// seg001, seg002, ...
		expected.Add(fmt.Sprintf("seg%03d", id))
	}

	// actual segment files
	actual := mapset.NewSet[string]()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name[:3] != "seg" {
			continue
		}

		actual.Add(name)
	}

	if res := actual.Difference(expected); res.Cardinality() != 0 {
		log.Printf("warning: orphaned segments exist: %v", res)
	}

	return nil
}
