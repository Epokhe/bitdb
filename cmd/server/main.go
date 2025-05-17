package main

import (
	"flag"
	"fmt"
	"github.com/epokhe/lsm-tree/cmd/remote"
	"github.com/epokhe/lsm-tree/core"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage:\n")
	fmt.Fprintf(os.Stderr, "  server -db <db-path>\n")
	os.Exit(1)
}

func main() {
	var (
		dbPath = flag.String("db", "", "path to data file")
		addr   = flag.String("addr", ":1234", "RPC listen address")
	)
	flag.Parse()

	if *dbPath == "" {
		usage()
	}

	// Open the database
	db, err := core.Open(*dbPath)
	if err != nil {
		log.Fatalf("could not open the database: %v", err)
	}

	// startRPC opens the DB, registers it, listens & serves.
	// Returns a cleanup func that closes the listener and DB.
	listenAddr, cleanup, err := remote.StartRPC(db, *addr)
	if err != nil {
		log.Fatalf("could not start RPC server: %v", err)
	}
	log.Printf("RPC server listening on %s", listenAddr)

	// Wait for SIGINT or SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down…")
	cleanup()
}
