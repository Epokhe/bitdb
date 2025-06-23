package core

import (
	"fmt"
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

func (db *DB) createMergeSegment() (*segment, error) {
	seg, err := newSegment(db.dir, db.claimNextSegmentId())
	if err != nil {
		return nil, fmt.Errorf("create merge segment %q: %w", seg.id, err)
	}

	return seg, nil
}

func (db *DB) rolloverSegment(out *mergeOutput) (*segment, error) {
	// create a new merge segment
	seg, err := db.createMergeSegment()
	if err != nil {
		return nil, err
	}

	out.segments = append(out.segments, seg)

	// i think we don't need it now because we will reset it in the end
	// reset the mergeIndex
	//out.index = make(map[string]*recordLocation)

	return seg, nil
}

func (db *DB) merge() error {
	// Acquire exclusive lock for the whole merge. You can optimise later.
	db.rw.Lock()
	defer db.rw.Unlock()

	// start with a new segment. processRecord will add more when needed
	out := newMergeOutput()
	mergeSeg, err := db.rolloverSegment(out)
	if err != nil {
		return err // todo errs
	}

	// we will only merge inactive segments because they are read-only
	// new segments added during the merge are out of scope
	inputLen := len(db.segments) - 1 // leave out last(active) segment
	for i := 0; i < inputLen; i++ {
		seg := db.segments[i]
		rs := newRecordScanner(seg)
		for rs.scan() {
			rec := rs.record

			// key should always exist in the index
			loc, _ := db.index[rec.key]

			// we will include latest occurrence the record
			// in the new segment and update the merge index
			isLatest := loc.seg == seg && loc.offset == rec.off

			// skip if not latest
			if !isLatest {
				continue
			}

			// prepare new segment if we grew over the limit
			// rollover should happen only when there's still
			// records left
			if mergeSeg.size > db.segmentSizeMax {
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
	}

	// ok we're done with processing existing segments

	// let's first finalize the segments
	for _, seg := range out.segments {
		if err = seg.finalize(); err != nil {
			return err
		}
	}

	// okay, remembering the issue here.
	// we have the input segments, we have to replace them with the new ones.
	// however, db.segments already had one more segment(active one), and now
	// it may have more than one additional segment because of new writes.
	// So, we will slice db.segments with input seg length, and replace that part.
	// todo lock here plsss
	db.segments = append(out.segments, db.segments[inputLen:]...)

	// todo lock here too. also we need to lock them together, otherwise
	//  index will have stale data.
	for k, v := range out.index {
		db.index[k] = v
	}

	// todo some locking shit needed for switching manifest etc.
	// todo put manifest overwrite somewhere xd
	if err := db.overwriteManifest(); err != nil {
		return fmt.Errorf("overwriteManifest: %w", err)
	}

	return nil
}
