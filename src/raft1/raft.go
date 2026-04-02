package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type role int

type logEntry struct {
	Term    int
	Command any
}

const (
	Follower role = iota
	Leader
	Candidate
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role         role
	currentTerm  int
	votedFor     int
	voteCount    int
	logBaseIndex int // log的基准索引，log[0]的索引
	snapshot     []byte
	log          []logEntry
	commitIndex  int
	lastApplied  int
	nextIndex    []int
	matchIndex   []int
	applyCh      chan raftapi.ApplyMsg
	timeStamp    time.Time     // 上次选举时间戳
	deadDuration time.Duration // 死亡时间间隔
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logBaseIndex)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []logEntry
	var logBaseIndex int
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logBaseIndex) != nil ||
		d.Decode(&log) != nil {
		panic("readPersist error")
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.logBaseIndex = logBaseIndex
		rf.log = log
		if rf.lastApplied < rf.logBaseIndex {
			rf.lastApplied = rf.logBaseIndex
		}
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果index小于等于logBaseIndex，说明这个快照已经过时了，不需要处理
	if index <= rf.logBaseIndex {
		return
	}
	relativeIndex := index - rf.logBaseIndex
	if relativeIndex < 0 || relativeIndex >= len(rf.log) {
		// log.Printf("Server %v: Snapshot index %v out of bounds", rf.me, index)
		return
	}
	// 截取log，从relativeIndex开始
	rf.log = rf.log[relativeIndex:]
	rf.logBaseIndex += relativeIndex
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logBaseIndex)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
	rf.snapshot = snapshot
}

func (rf *Raft) MakeSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	rf.snapshot = data
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	if args.LastIncludedIndex <= rf.logBaseIndex {
		reply.Term = rf.currentTerm
		return
	}

	// 截断日志
	if args.LastIncludedIndex > rf.logBaseIndex+len(rf.log)-1 {
		// 完全覆盖：日志全部丢弃，只剩一个占位
		rf.log = make([]logEntry, 1)
		rf.log[0] = logEntry{Term: args.LastIncludedTerm, Command: nil}
		rf.logBaseIndex = args.LastIncludedIndex
	} else {
		// 部分覆盖
		relativeIndex := args.LastIncludedIndex - rf.logBaseIndex
		rf.log = rf.log[relativeIndex:]
		rf.log[0] = logEntry{Term: args.LastIncludedTerm, Command: nil} 
		rf.logBaseIndex = args.LastIncludedIndex
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logBaseIndex)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	
	rf.persister.Save(raftstate, args.Data)
	rf.snapshot = args.Data

	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	
	reply.Term = rf.currentTerm
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	Candidate    int
	LastLogTerm  int
	LastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. 任期过时，拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	canVote := (rf.votedFor == -1 || rf.votedFor == args.Candidate)
	lastLogTerm := rf.log[len(rf.log)-1].Term
	lastLogIndex := rf.logBaseIndex + len(rf.log) - 1
	isUpToDate := false
	if args.LastLogTerm != lastLogTerm {
		isUpToDate = args.LastLogTerm > lastLogTerm
	} else {
		isUpToDate = args.LastLogIndex >= lastLogIndex
	}

	if canVote && isUpToDate {
		rf.votedFor = args.Candidate
		rf.persist()
		reply.VoteGranted = true
		rf.ResetHeartbeat() // 投票后重置计时器
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictLogIndex int // 冲突点的索引
	ConflictLogTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.ResetHeartbeat()
	rf.role = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.persist()

	lastLogIndex := rf.logBaseIndex + len(rf.log) - 1

	if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.ConflictLogIndex = lastLogIndex + 1
		return
	}

	if args.PrevLogIndex < rf.logBaseIndex {
		reply.Success = false
		reply.ConflictLogIndex = rf.logBaseIndex
		return
	}

	relPrevLogIndex := args.PrevLogIndex - rf.logBaseIndex
	
	if rf.log[relPrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictLogTerm = rf.log[relPrevLogIndex].Term
		reply.ConflictLogIndex = args.PrevLogIndex
		for reply.ConflictLogIndex > rf.logBaseIndex && 
		    rf.log[reply.ConflictLogIndex-1-rf.logBaseIndex].Term == reply.ConflictLogTerm {
			reply.ConflictLogIndex--
		}
		return
	}

	rf.log = rf.log[:relPrevLogIndex+1]
	
	if len(args.Entries) > 0 {
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logBaseIndex + len(rf.log) - 1)
	}

	reply.Success = true
}


func (rf *Raft) ResetHeartbeat() {
	rf.timeStamp = time.Now()
	rf.deadDuration = time.Duration(rand.Intn(100)+200) * time.Millisecond
}

func (rf *Raft) SendHeartbeat() {
	for {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				rf.mu.Lock()
				if rf.nextIndex[i] <= rf.logBaseIndex {
					args := &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.logBaseIndex,
						LastIncludedTerm:  rf.log[0].Term, // log[0] 肯定存在
						Data:              rf.persister.ReadSnapshot(),
					}
					reply := &InstallSnapshotReply{}
					rf.mu.Unlock()

					ok := rf.peers[i].Call("Raft.InstallSnapshot", args, reply)

					if ok {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.role = Follower
							rf.persist()
							rf.mu.Unlock()
							return
						}
						if reply.Term == rf.currentTerm {
							rf.matchIndex[i] = rf.logBaseIndex
							rf.nextIndex[i] = rf.logBaseIndex + 1
						}
						rf.mu.Unlock()
					}
					return
				}

			preLogIndex := rf.nextIndex[i] - 1
			if preLogIndex < rf.logBaseIndex || preLogIndex > rf.logBaseIndex+len(rf.log)-1 {
				if rf.nextIndex[i] <= rf.logBaseIndex {
					rf.mu.Unlock()
					return
				}
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.logBaseIndex,
					LastIncludedTerm:  rf.log[0].Term,
					Data:              rf.persister.ReadSnapshot(),
				}
				reply := &InstallSnapshotReply{}
				rf.mu.Unlock()

				ok := rf.peers[i].Call("Raft.InstallSnapshot", args, reply)
				if ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.role = Follower
						rf.persist()
						rf.mu.Unlock()
						return
					}
					if reply.Term == rf.currentTerm {
						rf.matchIndex[i] = rf.logBaseIndex
						rf.nextIndex[i] = rf.logBaseIndex + 1
					}
					rf.mu.Unlock()
				}
				return
			}
			args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[preLogIndex-rf.logBaseIndex].Term,
					LeaderCommit: rf.commitIndex,
				}
				if len(rf.log)-1 >= rf.nextIndex[i]-rf.logBaseIndex {
					entries := make([]logEntry, len(rf.log)-(rf.nextIndex[i]-rf.logBaseIndex))
					copy(entries, rf.log[rf.nextIndex[i]-rf.logBaseIndex:])
					args.Entries = entries
					//log.Printf("%v 发送日志给 %v, term: %v\n, entries: %v\n", rf.me, i, rf.currentTerm, entries)
				} else {
					args.Entries = nil
				}
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()
				ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
				if ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.role = Follower
						rf.persist()
//						log.Printf("%v 发现更新的任期, term: %v\n, 来自于leader %v\n", rf.me, reply.Term, reply)
						rf.mu.Unlock()
						return
					}
					// 任期过时，拒绝
					if reply.Term != rf.currentTerm {
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						// 成功，更新matchIndex和nextIndex
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
					} else {
						// 有冲突，回滚到冲突点
						rf.nextIndex[i] = reply.ConflictLogIndex
					}
					rf.mu.Unlock()
				}
				rf.UpdateCommitIndex()

			}(i)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) UpdateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 从后往前找可以提交的索引
	lastLogIndex := rf.logBaseIndex + len(rf.log) - 1
	for n := lastLogIndex; n > rf.commitIndex; n-- {
		if rf.log[n-rf.logBaseIndex].Term != rf.currentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			break
		}
	}
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.voteCount = 1
	rf.ResetHeartbeat()
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			lastLogIndex := rf.logBaseIndex + len(rf.log) - 1
			lastLogTerm := rf.log[len(rf.log)-1].Term
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				Candidate:    rf.me,
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)
			rf.mu.Lock()
			if ok {
				if reply.VoteGranted {
					rf.voteCount++
					if rf.voteCount > len(rf.peers)/2 && rf.role == Candidate {
						rf.role = Leader
						rf.voteCount = 0
						for j := range rf.peers {
							if j != rf.me {
								rf.nextIndex[j] = len(rf.log)
								rf.matchIndex[j] = 0
							}
						}
						rf.matchIndex[rf.me] = len(rf.log) - 1
						rf.persist()
						rf.ResetHeartbeat()
						go rf.SendHeartbeat()
					}
				} else if reply.Term > rf.currentTerm {
					rf.role = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					//log.Printf("%v 选举失败, term: %v\n, 来自于leader %v\n", rf.me, reply.Term, reply)
					rf.mu.Unlock()
					return
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) Applier() {
	for {
		msgs := make([]raftapi.ApplyMsg, 0)
		rf.mu.Lock()
		if rf.lastApplied < rf.logBaseIndex {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  rf.log[0].Term,
				SnapshotIndex: rf.logBaseIndex,
			}
			rf.applyCh <- msg
			rf.lastApplied = rf.logBaseIndex
			rf.mu.Unlock()
			continue
		}
		for rf.commitIndex > rf.lastApplied {
			relIdx := rf.lastApplied + 1 - rf.logBaseIndex
			if relIdx >= len(rf.log) {
				break
			}
			rf.lastApplied++
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[relIdx].Command,
				CommandIndex: rf.lastApplied,
			})
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	// Your code here (3B).
	if rf.role != Leader {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}
	logEntry := logEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	index = len(rf.log) + rf.logBaseIndex
	rf.matchIndex[rf.me] = index
	term = rf.currentTerm
	rf.log = append(rf.log, logEntry)
	rf.persist()
	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		now := time.Now()
		if now.Sub(rf.timeStamp) > rf.deadDuration {
			if rf.role != Leader {
				rf.mu.Unlock()
				go rf.StartElection()
			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		mu:           sync.Mutex{},
		peers:        peers,
		me:           me,
		dead:         0,
		role:         Follower,
		currentTerm:  0,
		votedFor:     -1,
		log:          make([]logEntry, 1),
		logBaseIndex: 0,
		persister:    persister,
		commitIndex:  0,
		lastApplied:  0,
		nextIndex:    make([]int, len(peers)),
		matchIndex:   make([]int, len(peers)),
		applyCh:      applyCh,
	}
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.Applier()

	return rf
}
