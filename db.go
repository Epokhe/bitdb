package main

import (
	"fmt"
	"os"
	"slices"
	s "strings"
)

var ErrKeyNotFound = fmt.Errorf("key not found")

func get(dbname string, key string) (string, error) {
	data, err := os.ReadFile(dbname)
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

func set(dbfile *os.File, key string, val string) error {
	serialized := fmt.Sprintf("%s,%s\n", key, val)
	_, err := dbfile.WriteString(serialized)
	return err
}
