package main

import (
	"fmt"
	"github.com/epokhe/lsm-tree/db"
	"os"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage:\n")
	fmt.Fprintf(os.Stderr, "  lsm-tree get <key>\n")
	fmt.Fprintf(os.Stderr, "  lsm-tree set <key> <value>\n")
	os.Exit(1)
}

func main() {

	dbPath := "main.db"
	// os.Args[0] is program name; we need at least action and key
	if len(os.Args) < 3 {
		usage()
	}

	action := os.Args[1]
	key := os.Args[2]

	switch action {
	case "get":
		if len(os.Args) != 3 {
			usage()
		}

		mainDb, err := db.Open(dbPath)
		if err != nil {
			// print to stderr, then exit with non‑zero code
			fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
			os.Exit(1)
		}
		defer mainDb.Close()

		val, err := mainDb.Get(key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get the key: %v\n", err)
		}

		fmt.Println(val)

	case "set":
		if len(os.Args) != 4 {
			usage()
		}
		val := os.Args[3]

		mainDb, err := db.Open(dbPath)
		if err != nil {
			// print to stderr, then exit with non‑zero code
			fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
			os.Exit(1)
		}
		defer mainDb.Close()

		err = mainDb.Set(key, val)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to set the key: %v\n", err)
			os.Exit(1)
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown action %q\n", action)
		usage()
	}

}
