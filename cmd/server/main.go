package main

import (
	"fmt"
	"github.com/epokhe/lsm-tree/db"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage:\n")
	fmt.Fprintf(os.Stderr, "  server <db-path>\n")
	os.Exit(1)
}

func main() {
	fmt.Println("Database started")

	if len(os.Args) != 2 {
		usage()
	}

	dbPath := os.Args[1]

	mainDb, err := db.Open(dbPath)
	if err != nil {
		// print to stderr, then exit with non‑zero code
		fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer mainDb.Close()

	err = rpc.Register(mainDb)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to register rpc: %v\n", err)
		os.Exit(1)
	}

	// List exactly what net/rpc has registered
	for _, m := range ListRegisteredMethods(rpc.DefaultServer) {
		fmt.Println(m)
	}

	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to listen: %v\n", err)
	}
	log.Printf("RPC server listening on %s", listener.Addr())

	// Graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down...")
		listener.Close() // stop accepting new conns

		// flush & close file
		if err := mainDb.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to persist to disk: %v\n", err)
			os.Exit(1)
		}

		os.Exit(0)
	}()

	rpc.Accept(listener)

}
