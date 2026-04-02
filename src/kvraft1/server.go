package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	data      map[string]*kvData
	lastOpIds map[int64]*Resp
	mu        sync.Mutex
}

type kvData struct {
	Value   string
	Version int64
}

type kvOpType int

const (
	GetOp kvOpType = iota
	PutOp
)

type Req struct {
	Id      int64
	Type    kvOpType
	Key     string
	Value   string
	Version int64
}

type Resp struct {
	Err     rpc.Err
	Version int64
	Value   string
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(op any) any {
	ops := op.(rsm.Op)
	request := ops.Req.(Req)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.lastOpIds[request.Id]; ok {
		return kv.lastOpIds[request.Id]
	}
	switch request.Type {
	case GetOp:
		if data, ok := kv.data[request.Key]; ok {
			kv.lastOpIds[request.Id] = &Resp{Value: data.Value, Version: data.Version, Err: rpc.OK}
			return kv.lastOpIds[request.Id]
		}
		kv.lastOpIds[request.Id] = &Resp{Err: rpc.ErrNoKey, Version: 0, Value: ""}
		return kv.lastOpIds[request.Id]
	case PutOp:
		if data, ok := kv.data[request.Key]; ok {
			if data.Version != request.Version {
				kv.lastOpIds[request.Id] = &Resp{Err: rpc.ErrVersion, Version: 0, Value: ""}
				return kv.lastOpIds[request.Id]
			}
			data.Value = request.Value
			data.Version++
			kv.lastOpIds[request.Id] = &Resp{Value: request.Value, Version: data.Version, Err: rpc.OK}
			return kv.lastOpIds[request.Id]
		}
		kv.data[request.Key] = &kvData{Value: request.Value, Version: 1}
		kv.lastOpIds[request.Id] = &Resp{Value: request.Value, Version: kv.data[request.Key].Version, Err: rpc.OK}
		return kv.lastOpIds[request.Id]
	default:
		kv.lastOpIds[request.Id] = &Resp{Err: rpc.ErrMaybe, Version: 0, Value: ""}
		return kv.lastOpIds[request.Id]
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Su
	// bmit() into a GetReply: rep.(rpc.GetReply)
	req := Req{
		Id:   args.Id,
		Type: GetOp,
		Key:  args.Key,
	}
	err, resp := kv.rsm.Submit(req)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	res := resp.(*Resp)
	reply.Err = res.Err
	reply.Version = rpc.Tversion(res.Version)
	reply.Value = res.Value
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	req := Req{
		Id:      args.Id,
		Type:    PutOp,
		Key:     args.Key,
		Value:   args.Value,
		Version: int64(args.Version),
	}
	err, resp := kv.rsm.Submit(req)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	res := resp.(*Resp)
	reply.Err = res.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(kvData{})
	labgob.Register(Req{})
	labgob.Register(Resp{})

	kv := &KVServer{
		me:        me,
		data:      make(map[string]*kvData),
		lastOpIds: make(map[int64]*Resp),
		mu:        sync.Mutex{},
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
