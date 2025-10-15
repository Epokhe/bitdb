package core

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type segmentStore struct {
	inactives []*segment
	active    *segment
	persist   bool     // whether to persist additions
	manifest  *os.File // open file handle for manifest
}

func initStore(existing []*segment, persistNew bool) *segmentStore {
	if len(existing) == 0 {
		log.Panicf("initStore should never be called with empty segment list")
	}
	last := len(existing) - 1
	return &segmentStore{inactives: existing[:last], active: existing[last], persist: persistNew}
}

func (store *segmentStore) add(new *segment) error {
	if store.active != nil {
		store.inactives = append(store.inactives, store.active)
	}

	store.active = new

	if err := store.overwriteManifest(); err != nil {
		return fmt.Errorf("overwrite manifest: %w", err)
	}

	return nil

}

func (store *segmentStore) all() []*segment {
	return append(store.inactives, store.active)
}

func ensureManifest(dir string) (mnf *os.File, new bool, rerr error) {
	manifestPath := filepath.Join(dir, "MANIFEST")

	_, err := os.Stat(manifestPath)
	if err != nil && !os.IsNotExist(err) {
		// Some other error trying to Stat
		return nil, false, fmt.Errorf("stat manifest: %w", err)
	}

	if os.IsNotExist(err) {
		// No manifest, let's create it
		mnf, err = createFileDurable(dir, "MANIFEST")
		if err != nil {
			return nil, true, fmt.Errorf("create manifest: %q", err)
		}

		return mnf, true, nil
	} else {
		// manifest already exists, return it
		mnf, err := os.OpenFile(manifestPath, os.O_RDWR, 0o644)
		if err != nil {
			return nil, false, fmt.Errorf("open manifest: %w", err)
		}
		return mnf, false, nil
	}
}

func (store *segmentStore) overwriteManifest() error {
	var buf bytes.Buffer

	// write inactive segments first
	for _, seg := range store.all() {
		fmt.Fprintf(&buf, "%d\n", seg.id)
	}

	if newf, err := writeFileAtomic(store.manifest, buf.Bytes()); err != nil {
		return fmt.Errorf("atomic write: %w", err)
	} else {
		store.manifest = newf
	}

	return nil
}
