package db

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"sync"
	"unsafe"
)

func ListRegisteredMethods(server *rpc.Server) []string {
	var methods []string

	// reflect.Value of the rpc.Server struct
	srvVal := reflect.ValueOf(server).Elem()

	// grab the unexported field named "serviceMap" (type sync.Map)
	smField := srvVal.FieldByName("serviceMap")
	// use unsafe to make it addressable & accessible
	sm := reflect.NewAt(smField.Type(), unsafe.Pointer(smField.UnsafeAddr())).
		Elem().Interface().(sync.Map)

	// Range over each registered service
	sm.Range(func(svcName, svcIface interface{}) bool {
		name := svcName.(string) // e.g. "DB"
		svcVal := reflect.ValueOf(svcIface).Elem()

		// grab the unexported "method" field (map[string]*methodType)
		mField := svcVal.FieldByName("method")
		mVal := reflect.NewAt(mField.Type(), unsafe.Pointer(mField.UnsafeAddr())).Elem()

		// iterate its keys (method names)
		for _, key := range mVal.MapKeys() {
			methods = append(methods, name+"."+key.String())
		}
		return true
	})

	return methods
}

func StartRPC(mainDb *DB, addr string) (listenAddr string, cleanup func(), err error) {
	// Register the rpc server
	server := rpc.NewServer()
	if err := server.RegisterName("DB", mainDb); err != nil {
		mainDb.Close()
		return "", nil, err
	}

	// List exactly what net/rpc has registered
	//for _, m := range ListRegisteredMethods(server) {
	//	fmt.Println(m)
	//}

	// Listen on TCP
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		mainDb.Close()
		return "", nil, err
	}

	// Serve in the background
	go server.Accept(listener)

	// Return the actual address and a cleanup callback
	cleanup = func() {
		listener.Close() // stop accepting new conns

		// flush & close file
		if err := mainDb.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to persist to disk: %v\n", err)
			os.Exit(1)
		}

	}
	return listener.Addr().String(), cleanup, nil
}
