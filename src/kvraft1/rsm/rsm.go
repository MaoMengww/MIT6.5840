package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int64
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	dead   chan struct{}
	result map[int64]chan any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		dead:         make(chan struct{}),
		result:       make(map[int64]chan any),
	}
	go rsm.reader()
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	// Extract Id from the request (Req type has Id field)
	id := int64(uuid.New().ID())
	op := Op{Me: rsm.me, Id: id, Req: req}
	ch := make(chan any)
	rsm.mu.Lock()
	rsm.result[id] = ch
	rsm.mu.Unlock()
	_, startTerm, leader := rsm.rf.Start(op)
	if !leader {
		return rpc.ErrWrongLeader, nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case msg := <-ch:
			return rpc.OK, msg
		case <-rsm.dead:
			return rpc.ErrMaybe, nil
		case <-ticker.C:
			currentTerm, isLeader := rsm.rf.GetState()
			if currentTerm != startTerm || !isLeader {
				rsm.mu.Lock()
				if cur, ok := rsm.result[id]; ok && cur == ch {
					delete(rsm.result, id)
				}
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			resp := rsm.sm.DoOp(op)
			rsm.mu.Lock()
			ch, ok := rsm.result[op.Id]
			rsm.mu.Unlock()
			if ok {
				ch <- resp
			}

		}
	}
	rsm.mu.Lock()
	for id, ch := range rsm.result {
		close(ch)
		delete(rsm.result, id)
	}
	rsm.mu.Unlock()
	rsm.dead <- struct{}{}
	close(rsm.dead)
}
