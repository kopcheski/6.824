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
	//	"bytes"
	"io"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"

	"github.com/sasha-s/go-deadlock"
)

var logIndex int32

type ServerState int

const (
	leader ServerState = iota
	candidate
	follower
)

func (s ServerState) String() string {
	switch s {
	case leader:
		return "leader"
	case candidate:
		return "candidate"
	case follower:
		return "follower"
	}
	return "unknown"
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu            deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	currentLeader int
	currentTerm   int
	votedTerms    map[int]string
	currentState  ServerState
	lastPing      time.Time
	applyCh       chan ApplyMsg
	log           []ApplyMsg          // the state machine, for commited messages
	tempLog       []ApplyMsg          // [ ] it is never cleaned up
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.amITheLeader()

	log.Printf("Hey server %d, who is the leader? %d", rf.me, rf.currentLeader)
	return term, isleader
}

func (rf *Raft) amITheLeader() bool {
	return rf.me == rf.currentLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []string
	LeaderCommit int
	Messages     []ApplyMsg
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Printf("Server %d will vote for term %d.", rf.me, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// [ ] it has to implement 5.4.1
	// it can also be starting an election itself
	// if rf.votedTerms[args.Term] == "voted" {
	// 	reply.VoteGranted = false
	// } else {
		reply.VoteGranted = args.Term > rf.currentTerm
		rf.votedTerms[args.Term] = "voted"
	// }
	log.Printf("Did server %d voted for server %d? %t.", rf.me, args.CandidateId, reply.VoteGranted)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (0 == len(args.Messages)) {
		// has to be in 'candidate' state too? pg6,3rd paragraph)
		// not a valid term, ignoring invalid leader.
		rf.voteForElection(args)
		return
	} else {
		for i, _ := range args.Messages {
			rf.tempLog = append(rf.tempLog, args.Messages[i])
		}
	}

	if (args.LeaderCommit > 0) {
		rf.commitMessages(args.LeaderCommit)
	}
}

func (rf *Raft) voteForElection(args *AppendEntriesArgs) {
	log.Printf("Server %d claims to be a leader for term %d.", args.LeaderId, args.Term)
	if args.Term >= rf.currentTerm {
		rf.currentState = follower
		rf.currentLeader = args.LeaderId
		rf.currentTerm = args.Term
		log.Printf("Claim of server %d was accepted. Server %d becomes now %s.",
			args.LeaderId, rf.me, rf.currentState)
	} else {
		log.Printf("Claim of server %d was declined.", args.LeaderId)

	}
	rf.lastPing = time.Now()
}

func (rf *Raft) fireElection() {
	log.Printf("Beginning election for term %d. Candidate: Server %d", rf.currentTerm, rf.me)
	
	rf.currentTerm += 1
	rf.currentState = candidate

	var votesFor = 1
	var totalVotesSoFar = 1
	rf.votedTerms[rf.currentTerm] = "voted"

	rf.mu.Lock()
	cond := sync.NewCond(&rf.mu)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			var reply = &RequestVoteReply{}
			var args = &RequestVoteArgs{}
			args.CandidateId = rf.me
			args.Term = rf.currentTerm
			log.Printf("Server %d is requesting vote to server %d", rf.me, peer)
			if (!rf.sendRequestVote(peer, args, reply)) {
				log.Printf("[FAILED] Server %d is requesting vote to server %d", rf.me, peer)
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.VoteGranted {
				votesFor += 1
			}
			totalVotesSoFar++

			if rf.isMajority(votesFor) {
				rf.currentState = leader
				cond.Broadcast()
			} else if rf.isMajorityImpossible(votesFor, totalVotesSoFar) {
				rf.currentState = follower
				log.Printf("Server %d WASN'T elected.", rf.me)
				cond.Broadcast()
			}
		}(i)
	}
	for (rf.currentState == candidate) {
		cond.Wait()
	}
	
	rf.mu.Unlock()

	if rf.isElected() {
		rf.becomeLeader()
	}
}

func (rf *Raft) becomeLeader() {
	log.Printf("Server %d WAS elected.", rf.me)
	rf.mu.Lock()
	rf.currentState = leader
	rf.currentLeader = rf.me
	rf.mu.Unlock()
	rf.claimAuthority()
}

func (rf *Raft) isElected() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentState == leader
}

func (rf *Raft) claimAuthority() {
	for rf.isElected() && !rf.killed() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(peer int) {
				var reply = &AppendEntriesReply{}
				var args = &AppendEntriesArgs{}
				args.LeaderId = rf.me
				args.Term = rf.currentTerm
				log.Printf("Server %d will claim its authority to server %d.", rf.me, peer)
				if (!rf.sendAppendEntries(peer, args, reply)) {
					log.Printf("[FAILED] Server %d will claim its authority to server %d.", rf.me, peer)
				}
			}(i)
		}
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
	log.Printf("Server %d state is %s", rf.me, rf.currentState)
}

func (rf *Raft) isMajority(occurrences int) bool {
	log.Printf("Server %d got %d votes from %d voters.", rf.me, occurrences, len(rf.peers))
	return occurrences >= rf.getMajority()
}

func (rf *Raft) isMajorityImpossible(votesPro int, votesSoFar int) bool {
	var potentialPro = votesPro + (len(rf.peers) - votesSoFar)
	return potentialPro < rf.getMajority()
}

func (rf *Raft) getMajority() int {
	var floatMajority = float64(len(rf.peers)) / 2.0
	var ceiledMajority = math.Ceil(floatMajority) 
	return int(ceiledMajority)
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.amITheLeader()
	if (isLeader) {
		index = int(atomic.AddInt32(&logIndex, 1))
		var msg = ApplyMsg{}
		msg.Command = command
		msg.CommandIndex = index
		// WHAT GOES HERE? msg.CommandValid
		rf.startAgreement(msg)
	}

	return index, term, isLeader
}

func (rf *Raft) startAgreement(msg ApplyMsg) {
	log.Printf("[%d] - starting agreement of index %d.", rf.me, msg.CommandIndex)
	cond := sync.NewCond(&rf.mu)

	rf.tempLog = append(rf.tempLog, msg)
	var replicationCount = 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			log.Printf("[%d] - replicating command to %d.", rf.me, peer)
			rf.replicateLog(peer, msg)
			replicationCount++
			cond.Broadcast()
		}(i)
	}
	for !rf.isMajority(replicationCount) {
		cond.Wait()
	}
	rf.commitMessages(msg.CommandIndex)
	log.Printf("[%d] - has replicated its logs to most of servers.", rf.me)

	go rf.replicateCommitToPeers(msg.CommandIndex)
	// [x] how to ask peers to commit?
          // - as above
	// [x] apply to my state machine
          // - done so when it goes to rf.log
	// [x] it tries to replicate to all followers indefinitely,
	// [x] but after replicating to the majority, it can then apply the change to its state machine.
	// [x] Finally, it returns to the client
}

func (rf *Raft) replicateCommitToPeers(index int) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicateCommit(peer, index)
	}
}

func (rf *Raft) replicateCommit(peer int, index int) {
	var reply = &AppendEntriesReply{}
	var args = &AppendEntriesArgs{}
	args.LeaderCommit = index
	if !rf.sendAppendEntries(peer, args, reply) {
		log.Printf("Server %d will retry replication of commit to server %d.", rf.me, peer)
		rf.replicateCommit(peer, index)
	}
}

func (rf *Raft) commitMessages(index int) {
	for _, v := range rf.tempLog {
		if v.CommandIndex <= index {
			rf.log = append(rf.log, v)
			v.CommandValid = true
			rf.applyCh <- v
		}
	}
}

func (rf *Raft) replicateLog(peer int, msg ApplyMsg) {
	// [x] AppendEntries to followers
	var reply = &AppendEntriesReply{}
	var args = &AppendEntriesArgs{}
	args.LeaderCommit = msg.CommandIndex
	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	args.Messages = append(args.Messages, msg)
	if (!rf.sendAppendEntries(peer, args, reply)) {
		log.Printf("Server %d will retry replication to server %d.", rf.me, peer)
		rf.replicateLog(peer, msg) // retry until it succeeds
	}
	log.Printf("Server %d has replicated its logs to server %d.", rf.me, peer)
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var tickerTimeout = randomRangeTimeout(500, 900)
	for !rf.killed() {
		time.Sleep(tickerTimeout)
		if rf.mustStartNewElection(tickerTimeout) {
			log.Printf("Server: %d. Elapsed %dms since last ping. Timeout: %d.", rf.me, time.Since(rf.lastPing).Milliseconds(), tickerTimeout.Milliseconds())
			rf.fireElection()
		}
	}
}

func (rf *Raft) mustStartNewElection(tickerTimeout time.Duration) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastPing) > tickerTimeout
}

func randomRangeTimeout(from int, to int) time.Duration {
	rand.Seed(time.Now().UnixNano())
	var duration = rand.Intn(to-from+1) + from
	return time.Duration(duration) * time.Millisecond
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
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if true {
		log.SetOutput(io.Discard)
	}
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentState = follower
	rf.currentTerm = 1
	rf.currentLeader = -1 // at this stage, it is unknown
	rf.votedTerms = make(map[int]string)
	rf.lastPing = time.Time{}
	rf.applyCh = applyCh

	log.Printf("Initializing server %d.", rf.me)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
