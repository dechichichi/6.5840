package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const ZeroVersion = rpc.Tversion(0)

type KVServer struct {
	mu       sync.Mutex
	kvalue   map[string]string
	kversion map[string]rpc.Tversion
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.kvalue = make(map[string]string)
	kv.kversion = make(map[string]rpc.Tversion)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, exist := kv.kvalue[args.Key]
	if !exist {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = val
	ver, exist := kv.kversion[args.Key]
	if !exist {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Version = ver
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ver, exist := kv.kversion[args.Key]
	if !exist {
		if args.Version == ZeroVersion {
			kv.kvalue[args.Key] = args.Value
			kv.kversion[args.Key] = args.Version + 1
			reply.Err = rpc.OK
			return
		}
		reply.Err = rpc.ErrNoKey
		return
	}
	if ver != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}
	kv.kvalue[args.Key] = args.Value
	kv.kversion[args.Key] = args.Version + 1
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
	for v := range kv.kvalue {
		delete(kv.kvalue, v)
	}
	for k := range kv.kversion {
		delete(kv.kversion, k)
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
