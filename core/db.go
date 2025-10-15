// Package core provides the core BitDB implementation.
package core

import (
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
	dir      string // data directory
	store    *segmentStore
	fsync    bool                       // whether to fsync on each Set call
	mergeSem chan struct{}              // merge semaphore
	rw       sync.RWMutex               // guards inactives & activeSeg & index & manifest
	mergeErr chan error                 // async merge error reporting
	idCtr    int64                      // segment id counter
	index    map[string]*recordLocation // maps each key to its last-seen location

	mergeEnabled      bool   // whether merge is enabled
	rolloverThreshold int64  // rollover segment when the active segment reaches this
	mergeThreshold    int    // run merge when inactive(merge-able) segment count reaches this
	checksumEnabled   bool   // enable corruption checks on Open and Get
	onMergeStart      func() // test hook
	onMergeApply      func() // test hook
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
			if err := db.AbortOpen(); err != nil {
				log.Printf("abort open: %v", err)
			}
		}
	}()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %q: %w", dir, err)
	}

	mnf, newManifest, err := ensureManifest(db.dir)
	if err != nil {
		return nil, fmt.Errorf("ensure manifest: %w", err)
	}
	db.store.manifest = mnf

	if newManifest {
		// in case this is a new folder, we create the first segment
		seg, err := newSegment(db.dir, db.claimNextSegmentId())
		if err != nil {
			return nil, fmt.Errorf("new segment: %w", err)
		}

		if err = db.store.addDurable(seg); err != nil {
			return nil, fmt.Errorf("create segment: %w", err)
		}
	} else {
		// we will load the segments ordered by the manifest file
		mnfBytes, err := io.ReadAll(db.store.manifest)
		if err != nil {
			return nil, fmt.Errorf("read manifest: %w", err)
		}

		// parse the manifest and get segment ids
		mnfIds := strings.Fields(string(mnfBytes))
		var segIds []int
		for _, idStr := range mnfIds {
			id, err := strconv.Atoi(idStr)
			if err != nil {
				// this error is unexpected unless manifest file gets corrupted
				return nil, fmt.Errorf("read manifest: %w", err)
			}
			segIds = append(segIds, id)
		}

		var segments []*segment
		for i := 0; i < len(segIds); i++ {
			id := segIds[i]
			seg, recs, err := parseSegment(db.dir, id, db.checksumEnabled)
			if err != nil {
				return nil, fmt.Errorf("parse segment %q: %w", id, err)
			}

			// update db index with the returned records
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

			segments = append(segments, seg)
		}

		db.store = initStore(segments, true)

		{
			s := db.store.active

			// for active segment, in case we have a corrupted record at the end,
			// we truncate to the last "good" offset
			if err := s.file.Truncate(s.size); err != nil {
				return nil, fmt.Errorf("truncate segment %d: %w", id, err)
			}

			// Go to the "new" end of the file in case it's truncated
			if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
				return nil, fmt.Errorf("seek on truncated segment %d: %w", id, err)
			}
		}

		// todo maybe move this to loadSegment, but need to change claimNextSegmentId
		//  or think about somehow moving this to segmentStore?? but need to share with merge part.
		// set the segment id counter
		maxId := 0
		if len(segIds) > 0 {
			maxId = slices.Max(segIds)
		}
		db.idCtr = int64(maxId + 1)

		if err = db.checkOrphanedSegments(segIds); err != nil {
			return nil, fmt.Errorf("check orphaned segments: %w", err)
		}
	}

	return db, nil
}

func getSegmentPath(dir string, id int) string {
	return filepath.Join(dir, fmt.Sprintf("seg%03d", id))
}

func (db *DB) claimNextSegmentId() int {
	// We atomically increment and return the previous value so callers always
	// get a unique id even under concurrency without needing external locks.
	return int(atomic.AddInt64(&db.idCtr, 1) - 1)
}

// todo segment storage with persistent bool, so we can use the same in merge

// creates a new active segment, moving the current active segment to inactive.
// triggers a manifest overwrite to make the change persistent.
// Assumes rw lock is taken.
func (db *DB) rolloverSegment() error {
	// seal current active segment and move to segments array
	if err := db.store.active.seal(); err != nil {
		return fmt.Errorf("seal segment %d: %w", db.store.active.id, err)
	}

	seg, err := newSegment(db.dir, db.claimNextSegmentId())
	if err != nil {
		return fmt.Errorf("new segment: %w", err)
	}

	db.store.addDurable(seg)
	if err := db.createSegment(); err != nil {
		return fmt.Errorf("create segment %d: %w", db.activeSeg.id, err)
	}

	return nil
}

//// Assumes rw lock is taken.
//func (store *segmentStore) createSegment() error {
//	// create the new active segment
//	seg, err := newSegment(db.dir, db.claimNextSegmentId())
//	if err != nil {
//		return fmt.Errorf("create new segment: %w", err)
//	}
//	db.segStore.add(seg)
//
//	if err := db.overwriteManifest(); err != nil {
//		return fmt.Errorf("overwrite manifest: %w", err)
//	}
//
//	return nil
//}

func (db *DB) Close() (errs error) {
	db.rw.Lock()
	defer db.rw.Unlock()

	// fsync and close the active segment
	if db.activeSeg != nil {
		if err := db.activeSeg.file.Sync(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("sync active segment %d: %w", db.activeSeg.id, err))
		}
		if err := db.activeSeg.file.Close(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("close active segment %d: %w", db.activeSeg.id, err))
		}
	}

	// close all inactive segments (all have scanFile)
	for _, s := range db.inactives {
		if err := s.file.Close(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("close segment %d: %w", s.id, err))
		}
		if err := s.scanFile.Close(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("close segment %d scan file: %w", s.id, err))
		}
	}

	// close the manifest
	if err := db.manifest.Close(); err != nil {
		errs = errors.Join(errs, fmt.Errorf("close manifest: %w", err))
	}

	return errs
}

// AbortOpen In case a failure happens during Open,
// we need to clean-up stuff opened so far. Keeping this
// separate from Close, which ensures graceful shutdown.
func (db *DB) AbortOpen() (errs error) {
	log.Println("open failed, releasing resources...")

	// close active segment if it was opened
	if db.activeSeg != nil && db.activeSeg.file != nil {
		if err := db.activeSeg.file.Close(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("close active segment %d: %w", db.activeSeg.id, err))
		}
	}

	// close all inactive segments which are opened so far
	for _, s := range db.inactives {
		if s.file != nil {
			if err := s.file.Close(); err != nil {
				errs = errors.Join(errs, fmt.Errorf("close segment %d: %w", s.id, err))
			}
		}
		if s.scanFile != nil {
			if err := s.scanFile.Close(); err != nil {
				errs = errors.Join(errs, fmt.Errorf("close segment %d scan file: %w", s.id, err))
			}
		}
	}

	// close the manifest if it was opened
	if db.manifest != nil {
		if err := db.manifest.Close(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("close manifest: %w", err))
		}
	}

	return errs
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
	if err := db.rolloverSegment(); err != nil {
		return fmt.Errorf("rollover segment: %w", err)
	}

	// threshold logic checks only inactive segments (activeSeg is separate now)
	if db.mergeEnabled && len(db.inactives) >= db.mergeThreshold {
		db.tryMerge()
	}

	return nil
}

func (db *DB) Set(key, val string) error {
	db.rw.Lock()
	defer db.rw.Unlock()

	// get active segment
	seg := db.activeSeg

	off, err := seg.write(key, val, TypeSet, db.fsync)
	if err != nil {
		return fmt.Errorf("write key %q on segment %d: %w", key, seg.id, err)
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
	seg := db.activeSeg

	if _, err := seg.write(key, "", TypeDelete, db.fsync); err != nil {
		return fmt.Errorf("write key %q on segment %d: %w", key, seg.id, err)
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

	// include active segment
	if db.activeSeg != nil {
		info, err := db.activeSeg.file.Stat()
		if err != nil {
			return 0, fmt.Errorf("stat active segment file: %w", err)
		}
		total += info.Size()
	}

	// include all inactive segments
	for _, seg := range db.inactives {
		info, err := seg.file.Stat()
		if err != nil {
			return 0, fmt.Errorf("stat segment file: %w", err)
		}
		total += info.Size()
	}
	return total, nil
}

// TotalSegmentCount returns the total number of segments (inactive + active)
func (db *DB) TotalSegmentCount() int {
	db.rw.RLock()
	defer db.rw.RUnlock()

	count := len(db.inactives)
	if db.activeSeg != nil {
		count++
	}
	return count
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
