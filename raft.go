package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Breeze-P/my-raft/labgob"
	"github.com/Breeze-P/my-raft/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

// 常量
const BROADCASTTIME = 100 // Millisecond

// utils
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// 结构体
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh chan ApplyMsg // An entry is considered committed if it is safe for that entry to be applied to state machines.
	// stopCh  chan struct{} // killed()替代方案

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role             int // 0 follower, 1 candidator, 2 leader
	isTheLeaderAlive bool

	currentTerm int        // 当前任期编号
	votedFor    int        // 投给了谁
	log         []LogEntry // first index is 1

	commonIndex int // 提交的commit（多数commit); 只增不减
	lastApplied int // 最后一个应用的log index

	nextIndex  []int // index是server编号，value是follower的index
	matchIndex []int // 每一个follower和match server的index

	snapshot    []byte
	baseIndex   int
	preLogEntry LogEntry
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	if index == rf.baseIndex-1 {
		return rf.preLogEntry
	}
	return rf.log[index-rf.baseIndex]
}

func (rf *Raft) getLen() int {
	return rf.baseIndex + len(rf.log)
}

func (rf *Raft) getTail() LogEntry {
	if len(rf.log) == 0 {
		return rf.preLogEntry
	}
	return rf.log[len(rf.log)-1]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock() // 在大写的函数里上锁
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == 2 && !rf.killed()
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

	// for snapshot
	e.Encode(rf.baseIndex)
	e.Encode(rf.preLogEntry)
	raftstate := w.Bytes()
	if rf.snapshot != nil {
		rf.persister.Save(raftstate, rf.snapshot)
		return
	}
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var baseIndex int
	var preLogEntry LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&baseIndex) != nil ||
		d.Decode(&preLogEntry) != nil {
		//   error...
		// fmt.Println("read wrong")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = make([]LogEntry, len(log))
		copy(rf.log, log)
		rf.baseIndex = baseIndex
		rf.commonIndex = baseIndex - 1
		rf.preLogEntry = preLogEntry
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.snapshot = make([]byte, len(data))
	copy(rf.snapshot, data)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including) <]
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	go rf.syncShot(index, snapshot)
}

func (rf *Raft) syncShot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 可能滞后
	if index <= rf.baseIndex {
		return
	}
	rf.preLogEntry = rf.getLogEntry(index)
	rf.log = rf.log[index+1-rf.baseIndex:]
	rf.snapshot = snapshot
	rf.baseIndex = index + 1
}

func (rf *Raft) toFollower(term int) {
	rf.isTheLeaderAlive = true
	rf.role = 0
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

// RPC Related

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	// Offset            int
	Data []byte
	// Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	// 如果小于commonIndex，说明是过期的Install
	if rf.commonIndex >= args.LastIncludedIndex {
		reply.Term = rf.currentTerm
		return
	}
	rf.commonIndex = max(rf.commonIndex, args.LastIncludedIndex)
	// Receive the entire snapshot.
	rf.snapshot = args.Data
	rf.preLogEntry = LogEntry{args.LastIncludedTerm, nil}

	// If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	if rf.getLen()-1 >= args.LastIncludedIndex && rf.getLogEntry(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		// retain log entries following it
		rf.log = rf.log[args.LastIncludedIndex+1-rf.baseIndex:]
		rf.baseIndex = args.LastIncludedIndex + 1
		rf.updateSnapshot(rf.snapshot, rf.preLogEntry.Term, rf.baseIndex-1)
		return
	}
	// Discard the entire log
	rf.log = rf.log[:0]
	rf.baseIndex = args.LastIncludedIndex + 1
	rf.updateSnapshot(rf.snapshot, rf.preLogEntry.Term, rf.baseIndex-1)
}

func (rf *Raft) sendInstallSnapshot(server, term, lastIncludedIndex, lastIncludedTerm int, snapshot []byte) {
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              snapshot,
	}
	reply := &InstallSnapshotReply{}
	for !rf.killed() {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		if ok {
			break
		}
	}
	if reply.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.toFollower(reply.Term)
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	rf.nextIndex[server] = max(lastIncludedIndex+1, rf.nextIndex[server])
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	Entries  []LogEntry

	PreLogIndex  int
	PreLogTerm   int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) consistencyCheck(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// If the follower does not find an entry in its log with the same index and term, then it refuses the new entries.
	// or
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.getLen()-1 < args.PreLogIndex {
		reply.Success = false
		reply.XLen = rf.getLen()
		return false
	}
	// 如果出现args.PreLogIndex < rf.baseIndex的情况是出现错误了，因为nextIndex一定大于leaderCommit大于已经snapshot的index的
	// may delay
	if rf.baseIndex > args.PreLogIndex+1 {
		return false
	}
	if rf.getLogEntry(args.PreLogIndex).Term != args.PreLogTerm {
		reply.XTerm = rf.getLogEntry(args.PreLogIndex).Term
		i := args.PreLogIndex - 1
		for ; i >= rf.baseIndex && rf.getLogEntry(i).Term == reply.XTerm; i-- { // find the first log with diff term
		}
		reply.XIndex = i + 1
		reply.XLen = rf.getLen()
		reply.Success = false
		return false
	}
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		// For Cond: If the term in the RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and con- tinues in candidate state.
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// args.Term == currentTerm 某cond胜选“establish its authority; at least as large as the candidate’s current term”
	if args.Term == rf.currentTerm {
		rf.isTheLeaderAlive = true
		rf.role = 0
	}
	// args.Term > currentTerm If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	if !rf.consistencyCheck(args, reply) {
		// If two entries in different logs have the same index and term, then the logs are identical in all preceding entries.
		return
	}
	// prevent new elections.
	rf.isTheLeaderAlive = true
	if len(args.Entries) > 0 && args.PreLogIndex+len(args.Entries) > rf.commonIndex { // 消息append
		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it
		if rf.getTail().Term == args.Term && rf.getLen()-1 > args.PreLogIndex+len(args.Entries) {
			// 同一term的过期请求
		} else {
			t := max(args.PreLogIndex, rf.commonIndex)
			t -= rf.baseIndex
			rf.log = rf.log[:t+1]
			rf.log = append(rf.log, args.Entries[max(0, rf.commonIndex-args.PreLogIndex):]...)
		}
	}
	if rf.commonIndex < args.LeaderCommit { // 包含心跳
		tmp := min(args.LeaderCommit, rf.getLen()-1)
		tmp = min(tmp, args.PreLogIndex) // for 更新不一定是append引发的，commonIndex以前的log内容不一定跟leader一致
		for index := rf.commonIndex + 1; index <= tmp; index++ {
			rf.updateApplyCh(true, rf.getLogEntry(index).Command, index) // todo snapshot之后可能会向下越界✅preIndex <= commonIndex
		}
		rf.commonIndex = max(tmp, rf.commonIndex)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.persist() // Updated on stable storage before responding to RPCs
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for !rf.killed() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			return
		}
	}
}

func (rf *Raft) sendSingleAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) leaderCompletenessProperty(args *RequestVoteArgs) bool {
	// the RPC includes information about the candidate’s log,
	// and the voter denies its vote if its own log is more up-to-date than that of the candidate.
	// 不能避免figure-8
	if rf.getTail().Term > args.LastLogTerm ||
		rf.getTail().Term == args.LastLogTerm && rf.getLen()-1 > args.LastLogIndex {
		return false
	}
	return true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm { // mw & 不保证reconnect cond变成follower，而恰恰相反，leader退位发起新一轮选举并保证其不会被选中
		// If a candidate discovers that its term is out of date, it immediately reverts to fol- lower state.
		rf.toFollower(args.Term)
	}
	// the Leader Completeness Property
	if !rf.leaderCompletenessProperty(args) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term < rf.currentTerm || rf.votedFor != -1 {
		//  If a server receives a request with a stale term number, it rejects the request.
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.persist() // Updated on stable storage before responding to RPCs
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
	index = rf.getLen() // from 1
	term = rf.currentTerm
	isLeader = rf.role == 2 && !rf.killed()
	if isLeader {
		// The leader appends the command to its log as a new entry
		rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
		go func(commond interface{}, index int, currentTerm int) {
			rf.appendNewEntries(commond, index, currentTerm)
		}(command, index, rf.currentTerm)
		rf.persist()
	}
	return index, term, isLeader
}

func (rf *Raft) updateApplyCh(ok bool, commond interface{}, index int) {
	rf.applyCh <- ApplyMsg{CommandValid: ok, Command: commond, CommandIndex: index}
}

func (rf *Raft) updateSnapshot(snapshot []byte, snapshotTerm int, snapshotIndex int) {
	rf.applyCh <- ApplyMsg{CommandValid: false, SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: snapshotTerm, SnapshotIndex: snapshotIndex}
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) appendNewEntries(command interface{}, index int, currentTerm int) {
	num := len(rf.peers)
	args := make([]*AppendEntriesArgs, num)
	replys := make([]*AppendEntriesReply, num) // 置空一个自己的
	var wg sync.WaitGroup
	wg.Add(num - 1)
	count := int32(1)
	// is- sues AppendEntries RPCs in parallel to each of the other servers to replicate the entry
	for i := range replys {
		if i == rf.me {
			continue
		}
		// Servers retry RPCs if they do not receive a re- sponse in a timely manner, and they issue RPCs in parallel for best performance.
		go func(i int, command interface{}, index int, currentTerm int) {
			defer wg.Done()
			// If followers crash or run slowly, or if network packets are lost, the leader retries Append- Entries RPCs indefinitely
			// (even after it has responded to the client) until all followers eventually store all log en- tries.
			for !rf.killed() {
				rf.mu.Lock()
				if rf.role != 2 || rf.currentTerm != currentTerm {
					rf.mu.Unlock()
					return
				}
				// 这里如果划到index，会出现nextIndex领先的情况
				// 划到len(rf.log)会出现实际推送到follower的log与leader commit的不一样，导致apply error
				if rf.nextIndex[i] > index {
					// 说明index之前的已经被follower接收
					rf.mu.Unlock()
					return
				}
				// have the leader send an InstallSnapshot RPC if it doesn't have the log entries required to bring a follower up to date.
				if rf.nextIndex[i] < rf.baseIndex {
					// create a separate goroutine to do the wait
					go rf.sendInstallSnapshot(i, rf.currentTerm, rf.baseIndex-1, rf.preLogEntry.Term, rf.snapshot)
					rf.mu.Unlock()
					return
				}
				args[i] = &AppendEntriesArgs{ // remake
					LeaderId:     rf.me,
					Term:         rf.currentTerm,
					Entries:      rf.log[rf.nextIndex[i]-rf.baseIndex:],
					LeaderCommit: rf.commonIndex,

					PreLogIndex: rf.nextIndex[i] - 1,
					PreLogTerm:  rf.getLogEntry(rf.nextIndex[i] - 1).Term, // 初始化有一个空的entry，所以不会越界；且一定有一个相同的即0号entry
				}
				replys[i] = &AppendEntriesReply{}
				// before sending an RPC (and waiting for the reply, the g should release the lock
				rf.mu.Unlock()
				rf.sendAppendEntries(i, args[i], replys[i])
				rf.mu.Lock()
				if replys[i].Term > rf.currentTerm { // mw
					rf.toFollower(replys[i].Term)
				}
				rf.mu.Unlock()
				if replys[i].Success {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// 可能会减小，但是无妨，危害就是follower的commit可能会滞后
					rf.nextIndex[i] = args[i].PreLogIndex + len(args[i].Entries) + 1 // 下一次心跳一定>=commonIndex
					// count++
					atomic.AddInt32(&count, 1)
					// A log entry is committed once the leader that created the entry has replicated it on a majority of the servers
					if atomic.LoadInt32(&count) > int32(num/2) {
						// This also commits【a preceding entries】in【the leader’s log】
						t := rf.nextIndex[i] - 1
						t = min(t, rf.getLen()-1)
						for i := rf.commonIndex + 1; i <= t; i++ {
							rf.updateApplyCh(true, rf.getLogEntry(i).Command, i)
						}
						tmp := min(t, rf.getLen()-1)              // 强制避免出错
						rf.commonIndex = max(rf.commonIndex, tmp) // 避免delay的resp roll back commonIndex
						rf.persist()
					}
					return
				} else {
					rf.mu.Lock()
					ori := rf.nextIndex[i]
					if replys[i].XTerm > 0 { // 有conflict
						flag, lastindex := rf.findTerm(replys[i].XTerm)
						if !flag {
							rf.nextIndex[i] = replys[i].XIndex
						} else {
							rf.nextIndex[i] = lastindex
						}
					}
					rf.nextIndex[i] = min(replys[i].XLen, rf.nextIndex[i])
					rf.nextIndex[i] = min(ori-1, rf.nextIndex[i]) // 至少减一个
					if rf.nextIndex[i] < 1 {                      // 强制避免错误
						rf.nextIndex[i] = 1
					}
					rf.mu.Unlock()
				}
			}
		}(i, command, index, currentTerm)
	}
	wg.Wait()
}

func (rf *Raft) findTerm(term int) (bool, int) {
	// find <=]
	l, r := rf.baseIndex, rf.getLen()-1
	for l <= r {
		mid := (l + r) / 2
		t := rf.getLogEntry(mid).Term
		if t <= term {
			l = mid + 1
		} else {
			r = mid - 1
		}
	}
	return rf.getLogEntry(r).Term == term, r
}

func (rf *Raft) startHeartBeats() {
	for rf.role == 2 {
		go rf.sendHeartBeats()
		ms := BROADCASTTIME
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeats() {
	num := len(rf.peers)
	args := make([]*AppendEntriesArgs, num)
	replys := make([]*AppendEntriesReply, num) // 置空一个自己的
	var wg sync.WaitGroup
	wg.Add(num - 1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			if rf.nextIndex[i] < rf.baseIndex || rf.nextIndex[i] > rf.getLen() { // 工程问题，非算法问题
				rf.mu.Unlock()
				return
			}
			// The leader keeps track of the highest index it knows to be committed,
			// and it includes that index in future AppendEntries RPCs【including heartbeats】
			// so that the other servers eventually find out.
			args[i] = &AppendEntriesArgs{
				LeaderId:     rf.me,
				Term:         rf.currentTerm,
				Entries:      []LogEntry{},
				LeaderCommit: rf.commonIndex,

				PreLogIndex: rf.nextIndex[i] - 1,
				PreLogTerm:  rf.getLogEntry(rf.nextIndex[i] - 1).Term, // 初始化有一个空的entry，所以不会越界；且一定有一个相同的即0号entry
			}
			replys[i] = &AppendEntriesReply{}
			rf.mu.Unlock()
			rf.sendSingleAppendEntries(i, args[i], replys[i])
			rf.mu.Lock()
			if replys[i].Term > rf.currentTerm {
				rf.toFollower(replys[i].Term)
			}
			rf.mu.Unlock()
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (rf *Raft) askForVotes() {
	rf.mu.Lock()
	rf.role = 1         // transitions to candidate state
	rf.currentTerm++    // increments its current term
	rf.votedFor = rf.me // votes for itself
	var lastLogTerm, lastLogIndex int
	lastLogTerm, lastLogIndex = rf.getTail().Term, rf.getLen()-1 // 0, 0 init
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}
	num := len(rf.peers)
	replys := make([]*RequestVoteReply, num) // 置空一个自己的
	for i := range replys {
		replys[i] = &RequestVoteReply{}
	}
	rf.persist() // votedFor、currentTerm改变了
	rf.mu.Unlock()
	var wg sync.WaitGroup
	wg.Add(num - 1)
	var count int32 = 1 // 票数, 初始有自己一票
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			// todo retry RPCs?
			ok := rf.sendRequestVote(i, args, replys[i])
			if ok {
				rf.mu.Lock()
				if replys[i].VoteGranted && rf.currentTerm == args.Term {
					count++
					// (a) it wins the election
					if count > int32(num/2) && rf.role == 1 { // rf.role == 1只comeToPower一次，避免reliable的resp把nextIndex清了
						go rf.comeToPower()
					}
				}
				// (b) another server establishes itself as leader
				if replys[i].Term > rf.currentTerm { // mw
					rf.toFollower(args.Term)
				}
				rf.mu.Unlock()
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (rf *Raft) comeToPower() {
	rf.role = 2
	rf.isTheLeaderAlive = true // 避免重新选举
	// When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.getLen()
	}
	go rf.startHeartBeats()
	rf.releaseBlankEntry()
}

func (rf *Raft) releaseBlankEntry() {
	rf.log = append(rf.log, LogEntry{rf.currentTerm, nil})
	go func(commond interface{}, index int, currentTerm int) {
		rf.appendNewEntries(commond, index, rf.currentTerm)
	}(nil, rf.getLen()-1, rf.currentTerm)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		// 前置waiting
		if rf.role != 2 {
			rf.isTheLeaderAlive = false
		}
		// before waiting for a timer, the g should release the lock
		rf.mu.Unlock()
		// election timeout
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if !rf.isTheLeaderAlive { // comeToPower&toFollower和心跳、append的时候都会be true
			go rf.askForVotes()
		}
		rf.mu.Unlock()
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.role = 0
	rf.isTheLeaderAlive = false
	rf.currentTerm = 0                    // 还没开始
	rf.votedFor = -1                      // 表示还没有选
	rf.log = []LogEntry{LogEntry{0, nil}} // 初始化空entry占位
	rf.commonIndex = 0                    // 0表示还没有提交任何log
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	// rf.stopCh = make(chan struct{})

	rf.snapshot = nil
	rf.baseIndex = 0
	rf.preLogEntry = LogEntry{0, nil}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
