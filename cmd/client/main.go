package main

import (
	"fmt"
	"github.com/epokhe/lsm-tree/cmd/remote"
	"log"
	"net/rpc"
	"os"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage:\n")
	fmt.Fprintf(os.Stderr, "  client get <key>\n")
	fmt.Fprintf(os.Stderr, "  client set <key> <value>\n")
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	action := os.Args[1]

	switch action {
	case "get":
		if len(os.Args) != 3 {
			usage()
		}
		key := os.Args[2]

		client, err := rpc.Dial("tcp", "localhost:1729")
		if err != nil {
			log.Fatalf("failed to dial rpc: %v\n", err)
		}
		var val string

		err = client.Call("DB.Get", &remote.GetArgs{Key: key}, &val)
		// todo don't give fatal if error is key not found
		if err != nil {
			log.Fatalf("failed to get the key: %v\n", err)
		}

		fmt.Println(val)

	case "set":
		if len(os.Args) != 4 {
			usage()
		}
		key := os.Args[2]
		val := os.Args[3]

		client, err := rpc.Dial("tcp", "localhost:1729")
		if err != nil {
			log.Fatalf("failed to dial rpc: %v\n", err)
		}

		var setReply struct{}

		err = client.Call("DB.Set", &remote.SetArgs{Key: key, Val: val}, &setReply)
		if err != nil {
			log.Fatalf("failed to set the key: %v\n", err)
		}

		fmt.Println("done")

	default:
		fmt.Fprintf(os.Stderr, "unknown action %q\n", action)
		usage()
	}

}
