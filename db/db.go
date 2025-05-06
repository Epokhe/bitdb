package db

import (
	"fmt"
	"os"
	"slices"
	s "strings"
)

type DB struct {
	path   string
	writer *os.File
}

var ErrKeyNotFound = fmt.Errorf("key not found")

func Open(path string) (*DB, error) {
	writer, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		return nil, err
	}

	return &DB{writer: writer, path: path}, nil
}

func (db *DB) Close() error {
	return db.writer.Close()
}

func (db *DB) Get(key string) (string, error) {
	data, err := os.ReadFile(db.path)
	if err != nil {
		return "", err
	}

	lines := s.Split(string(data), "\n")
	for _, line := range slices.Backward(lines) {
		//fmt.Println(i, line)
		k, v, found := s.Cut(line, ",")
		if found && k == key {
			return v, nil
		}

	}

	return "", ErrKeyNotFound
}

func (db *DB) Set(key string, val string) error {
	serialized := fmt.Sprintf("%s,%s\n", key, val)
	_, err := db.writer.WriteString(serialized)
	return err
}
