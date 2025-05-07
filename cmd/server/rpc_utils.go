package main

import (
	"net/rpc"
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
