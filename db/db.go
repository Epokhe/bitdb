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

type KeyNotFoundError struct {
	Key string
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("key %q not found", e.Key)
}

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

type GetArgs struct {
	Key string
}

func (db *DB) Get(args *GetArgs, reply *string) error {
	key := args.Key
	data, err := os.ReadFile(db.path)
	if err != nil {
		return err
	}

	lines := s.Split(string(data), "\n")
	for _, line := range slices.Backward(lines) {
		k, v, found := s.Cut(line, ",")
		if found && k == key {
			*reply = v
			return nil
		}
	}

	return &KeyNotFoundError{Key: key}
}

type SetArgs struct {
	Key string
	Val string
}

func (db *DB) Set(args *SetArgs, _ *struct{}) error {
	key := args.Key
	val := args.Val

	serialized := fmt.Sprintf("%s,%s\n", key, val)
	_, err := db.writer.WriteString(serialized)
	return err
}
