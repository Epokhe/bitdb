package core

import (
	"errors"
	"fmt"
	"log"
	"os"
)

type mergeOutput struct {
	segments     []*segment
	indexChanges map[string][2]*recordLocation
}

func newMergeOutput() *mergeOutput {
	return &mergeOutput{
		segments:     make([]*segment, 0),
		indexChanges: make(map[string][2]*recordLocation),
	}
}

func (db *DB) tryMerge() {
	// use a non-blocking send to acquire the semaphore
	select {
	case db.mergeSem <- struct{}{}:
		// run merge in a new goroutine
		go func() {
			if err := db.merge(); err != nil {
				db.mergeErr <- err
			}
			// release semaphore when there's no error
			<-db.mergeSem
		}()
	default:
		// merge already running
	}
}

func (db *DB) MergeErrors() <-chan error { return db.mergeErr }

func (db *DB) rolloverMergeSegment(out *mergeOutput) (*segment, error) {
	// create a new merge segment
	seg, err := newSegment(db.dir, db.claimNextSegmentId())
	if err != nil {
		return nil, fmt.Errorf("create merge segment: %w", err)
	}

	out.segments = append(out.segments, seg)
	return seg, nil
}

func (db *DB) merge() (rerr error) {
	// we will only merge inactive segments because they are read-only
	// new segments added during the merge are also out of scope
	db.rw.RLock()
	inputLen := len(db.segments) - 1 // leave out last(active) segment
	toMerge := db.segments[:inputLen]
	db.rw.RUnlock()

	// input segments are decided, run the callback for testing
	db.onMergeStart()

	out := newMergeOutput()

	defer func() {
		// in case of an unhandled error, we're rolling back
		// by removing all segments created for the merge
		if rerr != nil {
			if err := db.abortMerge(out); err != nil {
				log.Printf("abort merge: %v", err)
			}
		}
	}()

	mergeSeg, err := db.rolloverMergeSegment(out)
	if err != nil {
		return fmt.Errorf("rollover merge segment: %w", err)
	}

	for _, seg := range toMerge {
		// we don't do corruption checks on merge, there's not much point
		rs := newRecordScanner(seg.file, false)
		for rs.scan() {
			rec := rs.record

			db.rw.RLock()
			loc, ok := db.index[rec.key]
			db.rw.RUnlock()

			// db.index is guaranteed to be in a more recent state
			// than `toMerge` segments. so if `key` doesn't exist
			// in db.index, we can safely skip this record
			if !ok {
				continue
			}

			// we will include latest occurrence of the record
			// in the new segment and update the merge index
			isLatest := loc.seg == seg && loc.offset == rec.off

			// skip if not latest
			if !isLatest {
				continue
			}

			// prepare new segment if we grew over the limit
			// rollover should happen only when there's still
			// records left, that's why it's before write.
			if mergeSeg.size >= db.rolloverThreshold {
				if mergeSeg, err = db.rolloverMergeSegment(out); err != nil {
					return fmt.Errorf("rollover merge segment: %w", err)
				}
			}

			off, err := mergeSeg.write(rec.key, rec.val, TypeSet, db.fsync)
			if err != nil {
				return fmt.Errorf("write key %q on segment %d: %w", rec.key, mergeSeg.id, err)
			}

			// we memorize the both the old and the new location of the record
			// while merging to index, we need to make sure we're not replacing
			// a newer value of the key (explained below)
			out.indexChanges[rec.key] = [2]*recordLocation{loc, {
				seg:    mergeSeg,
				offset: off,
			}}
		}

		if err = rs.err; err != nil {
			return fmt.Errorf("scan segment %d: %w", seg.id, err)
		}
	}

	// ok we're done with processing existing segments

	// let's first finalize the segments
	for _, seg := range out.segments {
		if err := seg.file.Sync(); err != nil {
			return fmt.Errorf("sync segment %d: %w", seg.id, err)
		}
	}

	db.onMergeApply()

	// overwrite segments and index with one lock,
	// otherwise one will have stale data.
	db.rw.Lock()
	defer db.rw.Unlock()

	// merged segments replace their corresponding `inputLen` counterpart
	// and un-merged segments are appended
	db.segments = append(out.segments, db.segments[inputLen:]...)

	// overwrite index with merged entries
	// however, we should be careful about the updated keys
	// key may have been overwritten/deleted in the db
	// while we're busy with creating merge segments,
	// in that case we skip updating the key
	for key, locs := range out.indexChanges {
		curLoc, ok := db.index[key]
		if !ok {
			// deleted on db, skip
			continue
		}

		// if a new location for the record exists, it means this key
		// have been updated with a new value outside the merge process
		// we only update the index if this is the most recent location
		locBefore := locs[0] // to be replaced
		locAfter := locs[1]  // possible replacer

		isLatest := locBefore.seg == curLoc.seg && locBefore.offset == curLoc.offset
		if !isLatest {
			continue
		}

		// most recent. replace!
		db.index[key] = locAfter

	}

	if err := db.overwriteManifest(); err != nil {
		return fmt.Errorf("overwrite manifest: %w", err)
	}

	// remove old segment files; ignore errors and log them
	for _, seg := range toMerge {
		if err := seg.file.Close(); err != nil {
			log.Printf("close old segment %d: %v", seg.id, err)
		}

		if err := os.Remove(getSegmentPath(db.dir, seg.id)); err != nil {
			log.Printf("remove old segment %d: %v", seg.id, err)
		}
	}

	return nil
}

func (db *DB) abortMerge(out *mergeOutput) (errs error) {
	log.Println("merge failed, releasing resources...")

	for _, seg := range out.segments {
		if err := seg.file.Close(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("close segment %d: %w", seg.id, err))
		}

		if err := os.Remove(getSegmentPath(db.dir, seg.id)); err != nil {
			errs = errors.Join(errs, fmt.Errorf("remove segment %d: %w", seg.id, err))
		}
	}

	return errs
}
