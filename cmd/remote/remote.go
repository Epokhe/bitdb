package remote

import (
	"github.com/epokhe/lsm-tree/core"
	"log"
	"net"
	"net/rpc"
)

type DBRemote struct {
	db *core.DB
}

type SetArgs struct {
	Key string
	Val string
}

type GetArgs struct {
	Key string
}

func (remote *DBRemote) Get(args *GetArgs, reply *string) error {
	if val, err := remote.db.Get(args.Key); err != nil {
		return err
	} else {
		*reply = val
		return nil
	}
}

func (remote *DBRemote) Set(args *SetArgs, _ *struct{}) error {
	if err := remote.db.Set(args.Key, args.Val); err != nil {
		return err
	}
	return nil
}

func StartRPC(db *core.DB, addr string) (listenAddr string, cleanup func(), err error) {
	// Create the rpc object
	remote := &DBRemote{db: db}

	// Register the rpc server
	server := rpc.NewServer()

	if err := server.RegisterName("DB", remote); err != nil {
		db.Close()
		return "", nil, err
	}

	// Listen on TCP
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		db.Close()
		return "", nil, err
	}

	// Serve in the background
	go server.Accept(listener)

	// Return the actual address and a cleanup callback
	cleanup = func() {
		listener.Close() // stop accepting new conns

		// flush & close file
		if err := db.Close(); err != nil {
			log.Fatalf("failed to persist to disk: %v\n", err)
		}

	}
	return listener.Addr().String(), cleanup, nil
}
