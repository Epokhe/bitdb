package core

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type DB struct {
	path   string
	file   *os.File
	writer *bufio.Writer
	index  map[string]int64
	offset int64
}

var ErrKeyNotFound = errors.New("key not found")

func Open(path string) (*DB, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
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
	//we truncate to the last "good" offset
	if err := f.Truncate(offset); err != nil {
		return nil, err
	}

	// Go to the "new" end of the file in case it's truncated
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	w := bufio.NewWriter(f)

	return &DB{file: f, writer: w, path: path, index: index, offset: offset}, nil
}

func (db *DB) Close() error {
	// flush buffered bytes into the OS page cache
	// yesss, on power loss we lose these
	// ignored for now
	if err := db.writer.Flush(); err != nil {
		return err
	}

	// block until the OS has flushed those pages to stable storage
	if err := db.file.Sync(); err != nil {
		return err
	}

	// close the file
	return db.file.Close()
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

func (db *DB) Get(key string) (string, error) {

	recordOffset, ok := db.index[key]
	if !ok {
		// if not on index, the key doesn't exist
		return "", fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}

	val, err := db.readValueAt(recordOffset)
	if err != nil {
		// this is an unexpected error, because if key is on index,
		// its corresponding value should exist on the disk file
		return "", err
	}

	return val, nil
}

func (db *DB) Set(key, val string) error {

	// TODO:
	//  figure out why sometimes on ctrl+c it says file already closed

	// TODO:
	//  buffered writer doesn't flush until its buffer gets full.
	//  this means an indefinite wait until the process exits.
	//  we could have a ticker that periodically triggers a flush

	// TODO:
	//  we don't want to fsync at every write, but we also don't wanna lose
	//  any data. let's introduce a group commit option that can be used with low latency
	//  my guess is that it won't have a big impact in a high throughput scenario.

	// write key-value with length-prefix
	writeLen, err := writeKV(db.writer, key, val)

	if err != nil {
		return err
	}

	// flush into the OS page cache so ReadAt will see it
	// todo: this only guarantees read-after-write when no host failure happens
	//  in the future versions I have to also fsync
	//  but for that, I will do group commit
	// this costs 4us on average(set takes 34us). It's very low cost actually.
	if err := db.writer.Flush(); err != nil {
		return err
	}

	// I could use db.file.Sync() if I want fsync‐per‐write durability
	// fsync is crazy, it costs like 5ms. We could only accept this
	// in group commit scenario.

	// add offset to index
	// if power is lost just before this line, no prob,
	// index will be rebuilt anyway
	db.index[key] = db.offset

	// move offset by the written byte count
	db.offset += int64(writeLen)

	return err
}

// readValueAt reads back a single record at offset `off` in two syscalls:
//  1. ReadAt 8 bytes → header[0:4]==keyLen, header[4:8]==valLen
//  2. ReadAt keyLen+valLen bytes → payload
//
// I'm okay with two syscalls, no need to optimize them
// because they don't lead to two disk reads thanks to page cache
func (db *DB) readValueAt(off int64) (val string, err error) {
	// Read both lengths at once
	var hdr [8]byte
	if _, err = db.file.ReadAt(hdr[:], off); err != nil {
		return "", err
	}
	keyLen := int(binary.LittleEndian.Uint32(hdr[0:4]))
	valLen := int(binary.LittleEndian.Uint32(hdr[4:8]))

	// Read key+val in one go
	buf := make([]byte, valLen)
	if _, err = db.file.ReadAt(buf, off+8+int64(keyLen)); err != nil {
		return "", err
	}

	val = string(buf)
	return val, nil
}

// writeKV now emits:
//
//	[4-byte keyLen][4-byte valLen]  ← one 8-byte write
//	[key bytes]                      ← one write
//	[val bytes]                      ← one write
//
// returns the total length
func writeKV(w *bufio.Writer, key, val string) (totalLen int, err error) {
	// Build an 8-byte header on the stack
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(val)))

	// Write header
	_, err = w.Write(hdr[:])
	if err != nil {
		return totalLen, err
	}

	// Write key
	_, err = w.WriteString(key)
	if err != nil {
		return totalLen, err
	}

	// Write value
	_, err = w.WriteString(val)

	totalLen = 8 + len(key) + len(val)
	return totalLen, err
}
