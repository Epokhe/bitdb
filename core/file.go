package core

import (
	"os"
	"path/filepath"
)

// todo think of a better filename?

// writeFileAtomic atomically replaces the file with the full contents of data.
// It does so by writing to a temp file in the same directory, fsyncing it,
// renaming it over the old path, then fsyncing the directory.
//
// It returns the pointer to the new file handle.
func writeFileAtomic(f *os.File, data []byte) (*os.File, error) {
	path := f.Name()
	tmpPath := path + ".tmp"

	// on error, remove tmp file
	var err error
	defer func() {
		if err != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	// Create a temp file in the same directory
	// assuming {path}.tmp does not exist, else we will error out
	tmpf, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}

	// on error, remove tmp file handle
	defer func() {
		if err != nil {
			_ = tmpf.Close()
		}
	}()

	// Write all bytes at once
	if _, err = tmpf.Write(data); err != nil {
		return nil, err
	}

	// Sync the temp file to ensure data is on disk
	if err = tmpf.Sync(); err != nil {
		return nil, err
	}

	// Atomically rename temp file to its intended name
	if err = os.Rename(tmpPath, path); err != nil {
		return nil, err
	}

	// Close the old file handle
	if err = f.Close(); err != nil {
		return nil, err
	}

	// Finally, fsync the directory so the rename itself is durable
	dir := filepath.Dir(path)
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}

	defer d.Close() // nolint:errcheck

	if err = d.Sync(); err != nil {
		return nil, err
	}

	// Close tmp file handle because it points to wrong path
	_ = tmpf.Close()

	// Create new file handle that will be returned
	retf, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	return retf, nil
}

func createFileDurable(dir, name string) (*os.File, error) {
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	// fsync the file
	if err := f.Sync(); err != nil {
		return nil, err
	}

	// Fsync the directory so that the directory entry
	// is also committed to disk
	dfd, err := os.Open(dir)
	if err != nil {
		return nil, err
	}

	defer dfd.Close() // nolint:errcheck

	if err := dfd.Sync(); err != nil {
		return nil, err
	}

	// Now file definitely exists on disk and survives a crash.
	return f, nil
}
