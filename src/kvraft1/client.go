package kvraft

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int
	mu       sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {

	ck := &Clerk{clnt: clnt, servers: servers, leaderId: 0, mu: sync.Mutex{}}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	id := uuid.New().ID()
	args := &rpc.GetArgs{Id: int64(id), Key: key}

	for {
		reply := &rpc.GetReply{}
		ck.mu.Lock()
		ix := ck.leaderId
		ck.mu.Unlock()

		ok := ck.clnt.Call(ck.servers[ix], "KVServer.Get", args, reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.mu.Lock()
			ck.leaderId = ix
			ck.mu.Unlock()
			return reply.Value, reply.Version, reply.Err
		}
		ck.mu.Lock()
		ck.leaderId = (ix + 1) % len(ck.servers)
		ck.mu.Unlock()
	}
}

// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	id := uuid.New().ID()
	args := &rpc.PutArgs{
		Id:      int64(id),
		Key:     key,
		Value:   value,
		Version: version,
	}
	for {
		reply := &rpc.PutReply{}
		ck.mu.Lock()
		ix := ck.leaderId
		ck.mu.Unlock()

		ok := ck.clnt.Call(ck.servers[ix], "KVServer.Put", args, reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.mu.Lock()
			ck.leaderId = ix
			ck.mu.Unlock()
			return reply.Err
		}
		ck.mu.Lock()
		ck.leaderId = (ix + 1) % len(ck.servers)
		ck.mu.Unlock()
	}
}
