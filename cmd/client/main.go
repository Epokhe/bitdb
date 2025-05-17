package main

import (
	"fmt"
	rpc2 "github.com/epokhe/lsm-tree/cmd/rpc"
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

		client, err := rpc.Dial("tcp", "localhost:1234")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to dial rpc: %v\n", err)
			os.Exit(1)
		}
		var val string

		err = client.Call("DB.Get", &rpc2.GetArgs{Key: key}, &val)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get the key: %v\n", err)
			os.Exit(1)
		}

		fmt.Println(val)

	case "set":
		if len(os.Args) != 4 {
			usage()
		}
		key := os.Args[2]
		val := os.Args[3]

		client, err := rpc.Dial("tcp", "localhost:1234")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to dial rpc: %v\n", err)
			os.Exit(1)
		}

		var setReply struct{}

		err = client.Call("DB.Set", &rpc2.SetArgs{Key: key, Val: val}, &setReply)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to set the key: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("done")

	default:
		fmt.Fprintf(os.Stderr, "unknown action %q\n", action)
		usage()
	}

}
