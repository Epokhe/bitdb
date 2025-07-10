package core

import (
	"fmt"
	"log"
	"os"
)

type mergeOutput struct {
	segments []*segment
	index    map[string]*recordLocation
}

func newMergeOutput() *mergeOutput {
	return &mergeOutput{
		segments: make([]*segment, 0),
		index:    make(map[string]*recordLocation),
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

func (db *DB) rolloverSegment(out *mergeOutput) (*segment, error) {
	// create a new merge segment
	seg, err := newSegment(db.dir, db.claimNextSegmentId())
	if err != nil {
		return nil, fmt.Errorf("create merge segment: %w", err)
	}

	out.segments = append(out.segments, seg)
	return seg, nil
}

func (db *DB) merge() error {
	// we will only merge inactive segments because they are read-only
	// new segments added during the merge are also out of scope
	db.rw.RLock()
	inputLen := len(db.segments) - 1 // leave out last(active) segment
	toMerge := db.segments[:inputLen]
	db.rw.RUnlock()

	// input segments are decided, run the callback for testing
	db.onMergeStart()

	out := newMergeOutput()
	mergeSeg, err := db.rolloverSegment(out)
	if err != nil {
		return err // todo errs
	}

	for _, seg := range toMerge {
		rs := newRecordScanner(seg)
		for rs.scan() {
			rec := rs.record

			// key should always exist in the index
			db.rw.RLock()
			loc := db.index[rec.key]
			db.rw.RUnlock()

			// we will include latest occurrence the record
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
				if mergeSeg, err = db.rolloverSegment(out); err != nil {
					return err
				}
			}

			off, err := mergeSeg.write(rec.key, rec.val, db.fsync)
			if err != nil {
				return err
			}

			// update the mergeIndex
			out.index[rec.key] = &recordLocation{
				seg:    mergeSeg,
				offset: off,
			}
		}

		if err = rs.err; err != nil {
			return err
		}
	}

	// ok we're done with processing existing segments

	// let's first finalize the segments
	for _, seg := range out.segments {
		if err = seg.finalize(); err != nil {
			return err
		}
	}

	// overwrite segments and index with one lock,
	// otherwise one will have stale data.
	db.rw.Lock()
	defer db.rw.Unlock()

	// merged segments replace their corresponding `inputLen` counterpart
	// and un-merged segments are appended
	db.segments = append(out.segments, db.segments[inputLen:]...)

	// overwrite with merged entries
	for k, loc := range out.index {
		db.index[k] = loc
	}

	if err := db.overwriteManifest(); err != nil {
		return fmt.Errorf("overwriteManifest: %w", err)
	}

	// remove old segment files; ignore and log the errors
	for _, seg := range toMerge {
		if err := seg.file.Close(); err != nil {
			log.Printf("error removing old segments: file close: %v", err)
		}

		if err := os.Remove(getSegmentPath(db.dir, seg.id)); err != nil {
			log.Printf("error removing old segments: os.remove: %v", err)
		}
	}

	return nil
}
