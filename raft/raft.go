package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math"
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

type MemberStatus int
type EntryStatus int

const (
	Follower  MemberStatus = 0
	Candidate MemberStatus = 1
	Leader    MemberStatus = 2

	Entry EntryStatus = 0
	Snap  EntryStatus = 1
)

type LogEntry struct {
	Type     EntryStatus
	LogIndex int
	LogTerm  int
	Command  interface{}
}

type Snapshot struct {
	lastIncludedIndex int
	lastIncludedTerm  int
	data              []byte
}

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
	currentTerm      int
	votedFor         int
	status           MemberStatus
	currentTimeStamp time.Time
	lastHeardTime    time.Time
	votedCount       int

	log             []LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	applyCh         chan raftapi.ApplyMsg
	electionTimeout time.Duration

	electionInterval []int64
	lastAppendSent   []time.Time
	isValid          []bool
	// waitGr           sync.WaitGroup

	lastResponseGet []time.Time
	isValidRes      []bool

	snapshot Snapshot
}

func (rf *Raft) Get(index int) LogEntry {
	// fmt.Printf("Index=%d, lastIdx = %d, loglen=%d \n", index, rf.snapshot.lastIncludedIndex, len(rf.log))
	idx := index - rf.snapshot.lastIncludedIndex
	return rf.log[idx]
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
	isleader = (rf.status == Leader)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	// save snapshot metadata as part of raftstate
	e.Encode(rf.snapshot.lastIncludedIndex)
	e.Encode(rf.snapshot.lastIncludedTerm)
	raftstate := w.Bytes()
	// second argument is snapshot bytes
	rf.persister.Save(raftstate, rf.snapshot.data)
	// fmt.Printf("Presist: Server=%d snapIdx=%d snapTrm=%d\n", rf.me, rf.snapshot.lastIncludedIndex, rf.snapshot.lastIncludedTerm)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// If no saved raftstate, still try to read snapshot bytes
	if len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curTerm int
	var votedFor int
	var logEntries []LogEntry
	var snapIdx int
	var snapTerm int

	if d.Decode(&curTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil ||
		d.Decode(&snapIdx) != nil ||
		d.Decode(&snapTerm) != nil {
		// failed to decode; nothing to restore
		return
	}
	rf.currentTerm = curTerm
	rf.votedFor = votedFor
	rf.log = logEntries
	rf.snapshot.lastIncludedIndex = snapIdx
	rf.snapshot.lastIncludedTerm = snapTerm
	// snapshot bytes come from persister.ReadSnapshot()
	// fmt.Println("GGGGGG")
	rf.snapshot.data = rf.persister.ReadSnapshot()
	// fmt.Printf("after restart server %d has curTerm %d votedFor %d logsize %d snapIdx %d\n", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), snapIdx)
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
	shot := make([]byte, len(snapshot))
	copy(shot, snapshot)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// ignore if index already snapshotted OR index hasn't been applied yet
	if index <= rf.snapshot.lastIncludedIndex || index > rf.lastApplied {
		return
	}
	// fmt.Printf("index=%d server=%d logLen=%d, commitIdx=%d applied=%d\n", index, rf.me, len(rf.log), rf.commitIndex, rf.lastApplied)

	rf.snapshot.lastIncludedTerm = rf.Get(index).LogTerm

	newLog := []LogEntry{} //make([]LogEntry, len(rf.log)-(index+1)+1)
	newLog = append(newLog, LogEntry{Type: Snap, LogIndex: index, LogTerm: rf.snapshot.lastIncludedTerm, Command: nil})
	// fmt.Println(index - rf.snapshot.lastIncludedIndex + 1)
	newLog = append(newLog, rf.log[index-rf.snapshot.lastIncludedIndex+1:]...)
	rf.log = newLog

	rf.snapshot.lastIncludedIndex = index
	rf.snapshot.data = shot
	rf.persist()
	// fmt.Printf("snap : index=%d server=%d logLen=%d, commitIdx=%d applied=%d\n", index, rf.me, len(rf.log), rf.commitIndex, rf.lastApplied)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	SentTime          string
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// fmt.Println("opaaa")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	sentTime, _ := time.Parse(time.RFC3339Nano, args.SentTime)
	leaderId := args.LeaderId

	if rf.isValid[leaderId] {
		if rf.lastAppendSent[leaderId].After(sentTime) {
			// fmt.Println("blaaaaa")
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}
	} else {
		rf.isValid[leaderId] = true
	}

	rf.lastAppendSent[leaderId] = sentTime

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	defer rf.persist()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower
	}

	if args.LastIncludedIndex <= rf.snapshot.lastIncludedIndex {
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}

	rem_elems := []LogEntry{}

	if rf.log[len(rf.log)-1].LogIndex >= args.LastIncludedIndex {
		rem_elems = rf.log[(args.LastIncludedIndex - rf.snapshot.lastIncludedIndex + 1):]
	}

	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{Type: Snap, LogIndex: args.LastIncludedIndex, LogTerm: args.LastIncludedTerm, Command: nil})
	rf.log = append(rf.log, rem_elems...)

	rf.commitIndex = int(math.Max(float64(rf.commitIndex), float64(args.LastIncludedIndex)))

	snapCopy := make([]byte, len(args.Data))
	copy(snapCopy, args.Data)
	rf.snapshot.data = snapCopy
	rf.snapshot.lastIncludedIndex = args.LastIncludedIndex
	rf.snapshot.lastIncludedTerm = args.LastIncludedTerm

	reply.Term = rf.currentTerm
	reply.Success = true
	if args.LastIncludedIndex <= rf.lastApplied {
		return
	}

	rf.lastApplied = args.LastIncludedIndex - 1
}

func sendSnapshot(rf *Raft, server int) {
	args := &InstallSnapshotArgs{}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.snapshot.lastIncludedIndex
	args.LastIncludedTerm = rf.snapshot.lastIncludedTerm
	args.Data = rf.snapshot.data
	args.SentTime = time.Now().Format(time.RFC3339Nano)
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(server, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != Leader {
		return
	}

	sentTime, _ := time.Parse(time.RFC3339Nano, args.SentTime)

	if rf.isValidRes[server] {
		if rf.lastResponseGet[server].After(sentTime) {
			fmt.Println("bluuu")
			return
		}
	} else {
		rf.isValid[server] = true
	}

	rf.lastAppendSent[server] = sentTime
	defer rf.persist()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.status = Follower
		return
	}

	if reply.Success {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}

}

type AppendEntryArgs struct {
	Term     int
	LeaderId int
	// Your data here (2A, 2B).
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	SentTime     string
}

const (
	Ok          int = 0
	LogShort    int = 1
	Ignore      int = 2
	HitSnapshot int = 3
)

type AppendEntryReply struct {
	Term    int
	Success bool

	LogLen     int
	ConfTerm   int
	ConfIdx    int
	ConfStatus int
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// fmt.Println("AppendEntry", rf.me, len(rf.log))
	sentTime, _ := time.Parse(time.RFC3339Nano, args.SentTime)
	leaderId := args.LeaderId

	if rf.isValid[leaderId] {
		if rf.lastAppendSent[leaderId].After(sentTime) {
			// fmt.Println("blaaaaa")
			reply.Success = false
			reply.ConfStatus = Ignore
			reply.Term = rf.currentTerm
			return
		}
	} else {
		rf.isValid[leaderId] = true
	}

	rf.lastAppendSent[leaderId] = sentTime

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConfStatus = Ignore
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower
	}

	// fmt.Printf("V4 sever=%d, commidIdx=%d, logLen=%d, from=%d prevLogIdx=%d entryLen=%d sendTIme=%s\n", rf.me, rf.commitIndex, len(rf.log), args.LeaderId, args.PrevLogIndex, len(args.Entries), args.SentTime)

	rf.lastHeardTime = time.Now()
	// reset election timeout on valid leader contact
	rf.electionTimeout = time.Duration(rf.electionInterval[0]+rand.Int63()%rf.electionInterval[1]) * time.Millisecond

	reply.Term = rf.currentTerm

	logLen := rf.log[len(rf.log)-1].LogIndex + 1

	if args.PrevLogIndex >= logLen {
		reply.Success = false
		reply.LogLen = logLen
		reply.ConfStatus = LogShort
		// fmt.Printf("failed log Len\n")
		return
	}

	if args.PrevLogIndex >= 0 && args.PrevLogIndex < logLen && args.PrevLogIndex >= rf.snapshot.lastIncludedIndex && rf.Get(args.PrevLogIndex).LogTerm != args.PrevLogTerm {
		reply.Success = false
		reply.ConfStatus = Ok
		reply.LogLen = logLen
		reply.ConfTerm = rf.Get(args.PrevLogIndex).LogTerm
		i := args.PrevLogIndex
		for i >= rf.snapshot.lastIncludedIndex && rf.Get(i).LogTerm == reply.ConfTerm {
			i--
		}
		reply.ConfIdx = i + 1

		if rf.Get(i+1).Type == Snap {
			reply.ConfStatus = HitSnapshot
		}
		// fmt.Println("Failed wrong entry")
		return
	}

	// fmt.Printf("V3 sever=%d, commidIdx=%d, leaderCommit=%d,logLen=%d, from=%d prevLogIdx=%d entryLen=%d sendTIme=%s\n", rf.me, rf.commitIndex, args.LeaderCommit, len(rf.log), args.LeaderId, args.PrevLogIndex, len(args.Entries), args.SentTime)

	// last_len := len(rf.log)
	if len(args.Entries) > 0 {
		if args.PrevLogIndex < rf.snapshot.lastIncludedIndex {
			snapIdx := rf.snapshot.lastIncludedIndex
			i := 0
			for args.Entries[i].LogIndex != snapIdx {
				i++
			}

			rf.log = rf.log[:1]
			rf.log = append(rf.log, args.Entries[(i+1):]...)
		} else {
			newIndex := args.PrevLogIndex + 1 - rf.snapshot.lastIncludedIndex
			if newIndex < len(rf.log) {
				rf.log = rf.log[:newIndex]
			}
			rf.log = append(rf.log, args.Entries...)
			if rf.commitIndex >= len(rf.log) {
				// fmt.Printf("Error sever=%d, commidIdx=%d, logLen=%d, from=%d prevLogIdx=%d entryLen=%d sendTIme=%s\n", rf.me, rf.commitIndex, len(rf.log), args.LeaderId, args.PrevLogIndex, len(args.Entries), args.SentTime)

			} else {

			}
			// fmt.Printf("V2 sever=%d, commidIdx=%d, leaderCommit=%d, logLen=%d, from=%d prevLogIdx=%d entryLen=%d sendTIme=%s\n", rf.me, rf.commitIndex, args.LeaderCommit, len(rf.log), args.LeaderId, args.PrevLogIndex, len(args.Entries), args.SentTime)
		}

	}

	// cur_len := len(rf.log)

	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex := rf.log[len(rf.log)-1].LogIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastLogIndex)))
		// fmt.Printf("V1 sever=%d, commidIdx=%d, logLen=%d,from=%d\n", rf.me, rf.commitIndex, len(rf.log), args.LeaderId)
	}

	reply.Success = true
	// if len(args.Entries) > 0 {
	// 	fmt.Printf("follower %d appended successfuly %d %d\n", rf.me, args.PrevLogIndex, args.PrevLogTerm)

	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func cmpLogs(rf *Raft, lastLogIndex2 int, lastLogTerm2 int) int {
	lastLogIndex := rf.log[len(rf.log)-1].LogIndex
	lastLogTerm := rf.Get(lastLogIndex).LogTerm

	if lastLogTerm == lastLogTerm2 {
		return lastLogIndex2 - lastLogIndex
	}
	return lastLogTerm2 - lastLogTerm

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower
	}

	rf.lastHeardTime = time.Now()

	// fmt.Println("me from myIdx myTerm hisIdx hisTers", rf.me, args.CandidateId, len(rf.log)-1,
	// 	rf.log[len(rf.log)-1].LogTerm, args.LastLogIndex, args.LastLogTerm)

	rf.electionTimeout = time.Duration(rf.electionInterval[0]+rand.Int63()%rf.electionInterval[1]) * time.Millisecond

	reply.Term = rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (cmpLogs(rf, args.LastLogIndex, args.LastLogTerm) >= 0) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
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
	defer rf.mu.Unlock()
	defer rf.persist()
	// Your code here (3B).
	if rf.status != Leader {
		return index, term, false
	}
	index = rf.log[len(rf.log)-1].LogIndex + 1
	rf.log = append(rf.log, LogEntry{Type: Entry, LogTerm: rf.currentTerm, LogIndex: index, Command: command})
	term = rf.currentTerm
	isLeader = true
	// fmt.Printf("Start: term %d idx %d leader %d lggLen=%d snapIDx=%d\n", term, index, rf.me, len(rf.log), rf.snapshot.lastIncludedIndex)
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

func sendHeartbeat(rf *Raft, server int) {
	// defer rf.waitGr.Done()
	args := &AppendEntryArgs{}
	rf.mu.Lock()

	if rf.status != Leader {
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[server] <= rf.snapshot.lastIncludedIndex {
		rf.mu.Unlock()
		// send installSnapshot
		sendSnapshot(rf, server)
		return
	}

	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.Get(args.PrevLogIndex).LogTerm
	args.Entries = rf.log[rf.nextIndex[server]-rf.snapshot.lastIncludedIndex:]
	args.LeaderCommit = rf.commitIndex
	args.SentTime = time.Now().Format(time.RFC3339Nano)
	rf.mu.Unlock()

	reply := &AppendEntryReply{}

	ok := rf.sendAppendEntry(server, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != Leader {
		// fmt.Println("Upppp ", reply.Success, len(args.Entries))
		return
	}

	sentTime, _ := time.Parse(time.RFC3339Nano, args.SentTime)

	if rf.isValidRes[server] {
		if rf.lastResponseGet[server].After(sentTime) {
			fmt.Println("bluuu")
			return
		}
	} else {
		rf.isValid[server] = true
	}

	rf.lastAppendSent[server] = sentTime
	defer rf.persist()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.status = Follower
		return
	}

	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		// fmt.Printf("HeartBeat: server=%d, commitIdx=%d, nextIdx=%d,matchindex=%d\n", server, rf.commitIndex, rf.nextIndex[server], rf.matchIndex[server])
		// if len(args.Entries) > 0 {
		// 	rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
		// 	rf.matchIndex[server] = rf.nextIndex[server] - 1
		// } else {
		// 	if args.PrevLogIndex > rf.matchIndex[server] {
		// 		rf.matchIndex[server] = args.PrevLogIndex
		// 	}
		// }

		for N := rf.commitIndex + 1; N < rf.log[len(rf.log)-1].LogIndex+1; N++ {
			if rf.Get(N).LogTerm != rf.currentTerm {
				// fmt.Println("blaaa")
				continue
			}
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				// fmt.Printf("Server=%d, commitIdx=%d, logLen=%d\n", rf.me, rf.commitIndex, len(rf.log))
			}
		}
	} else {
		if rf.nextIndex[server] > 1 {
			if reply.ConfStatus == Ignore {
				return
			}
			// rf.nextIndex[server]--

			if reply.ConfStatus == HitSnapshot {
				rf.nextIndex[server] = reply.ConfIdx
				return
			}

			if reply.ConfStatus == LogShort {
				rf.nextIndex[server] = reply.LogLen
				return
			}

			i := rf.log[len(rf.log)-1].LogIndex
			for i >= rf.snapshot.lastIncludedIndex && rf.Get(i).LogTerm != reply.ConfTerm {
				i--
			}

			if i != -1 {
				rf.nextIndex[server] = i + 1
			} else {
				rf.nextIndex[server] = reply.ConfIdx
			}
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// fmt.Println("entered")
		// Apply all committed entries that haven't been applied yet
		var toApply []raftapi.ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			if rf.lastApplied < rf.snapshot.lastIncludedIndex {
				rf.lastApplied = rf.snapshot.lastIncludedIndex
			}

			entry := rf.Get(rf.lastApplied)
			// fmt.Printf("Applyer: server=%d, applyIdx=%d ,snapIdx=%d, snapTerm=%d, logLen=%d\n", rf.me, rf.lastApplied, rf.snapshot.lastIncludedIndex, rf.snapshot.lastIncludedTerm, len(rf.log))
			if entry.Type == Snap {
				rf.applyCh <- raftapi.ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.snapshot.data,
					SnapshotTerm:  rf.snapshot.lastIncludedTerm,
					SnapshotIndex: rf.snapshot.lastIncludedIndex,
				}
			} else {
				toApply = append(toApply, raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied,
				})
			}

		}
		// fmt.Println("out")
		rf.mu.Unlock()

		// Send to applyCh (outside lock to avoid deadlock)
		for _, msg := range toApply {
			rf.applyCh <- msg
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func lead(rf *Raft) {
	// fmt.Printf("Lead=%d, logLen=%d \n", rf.me, len(rf.log))
	for {
		if rf.killed() {
			return
		}

		_, isLeader := rf.GetState()
		if isLeader {
			// send AppendEntries RPCs to all followers
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					// rf.waitGr.Add(1)
					go sendHeartbeat(rf, i)
				}
			}
			// rf.waitGr.Wait()
		} else {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

}

func sendRequestVote(rf *Raft, server int) {
	args := &RequestVoteArgs{}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.log[len(rf.log)-1].LogIndex
	args.LastLogTerm = rf.log[len(rf.log)-1].LogTerm
	rf.mu.Unlock()

	reply := &RequestVoteReply{}

	ok := rf.sendRequestVote(server, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.status = Follower
		return
	}

	if reply.VoteGranted && rf.status == Candidate && args.Term == rf.currentTerm {
		rf.votedCount += 1
		if rf.votedCount > len(rf.peers)/2 {
			rf.status = Leader
			// initialize leader state here
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.log[len(rf.log)-1].LogIndex + 1
				rf.matchIndex[i] = 0
				rf.isValid[i] = false
				rf.isValidRes[i] = false
			}
			// Leader's own matchIndex is the last log index
			rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].LogIndex

			// fmt.Printf("term %d server %d became Leader\n", rf.currentTerm, rf.me)
			go lead(rf)
		}
	}
}

func gainLidership(rf *Raft) {
	// Your code here (3A).
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// send RequestVote RPCs to all other servers
			go sendRequestVote(rf, i)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		startedElection := false
		rf.mu.Lock()
		// trigger election if follower/candidate has not heard from leader within timeout
		if rf.status != Leader {
			if time.Since(rf.lastHeardTime) >= rf.electionTimeout {
				rf.status = Candidate
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.votedCount = 1
				rf.lastHeardTime = time.Now()
				rf.electionTimeout = time.Duration(rf.electionInterval[0]+rand.Int63()%rf.electionInterval[1]) * time.Millisecond

				startedElection = true
			}
		}
		rf.mu.Unlock()

		if startedElection {
			go gainLidership(rf)
		}

		// small sleep to avoid busy loop
		time.Sleep(20 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	// fmt.Println("AAAAAAAAAAAAAAAAAAAAa", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.status = Follower
	rf.currentTimeStamp = time.Now()
	time.Sleep(time.Millisecond)
	rf.lastHeardTime = time.Now()
	rf.electionInterval = []int64{300, 200}
	rf.electionTimeout = time.Duration(rf.electionInterval[0]+rand.Int63()%rf.electionInterval[1]) * time.Millisecond

	rf.log = []LogEntry{}
	rf.matchIndex = []int{}
	rf.isValid = make([]bool, len(rf.peers))
	rf.lastAppendSent = make([]time.Time, len(rf.peers))
	rf.isValidRes = make([]bool, len(rf.peers))
	rf.lastResponseGet = make([]time.Time, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.isValid[i] = true
	}

	rf.snapshot.lastIncludedIndex = 0
	rf.snapshot.lastIncludedTerm = 0
	rf.snapshot.data = nil

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	// fmt.Println("Upaaaa")
	rf.readPersist(persister.ReadRaftState())
	// fmt.Println("Opaaaaa")
	if len(rf.log) == 0 {
		rf.log = append(rf.log, LogEntry{Snap, 0, 0, nil})
	}

	if rf.snapshot.lastIncludedIndex != 0 {
		rf.commitIndex = rf.snapshot.lastIncludedIndex
		rf.lastApplied = rf.snapshot.lastIncludedIndex - 1
		// rf.applyCh <- raftapi.ApplyMsg{
		// 	SnapshotValid: true,
		// 	Snapshot:      rf.snapshot.data,
		// 	SnapshotTerm:  rf.snapshot.lastIncludedTerm,
		// 	SnapshotIndex: rf.snapshot.lastIncludedIndex,
		// }
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	// start apply goroutine to apply committed entries
	go rf.applier()

	return rf
}
