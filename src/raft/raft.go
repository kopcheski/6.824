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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// applyChan
	applyCh chan ApplyMsg

	currentTerm   int
	voteFor       int
	roleStatus    int // 0: follower   1: candidate  2:leader
	timeoutLastTS time.Time
	log           []LogEntry // the first index is 1
	commitIndex   int
	lastApplied   int
	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int
	// Personal Setting
	loggerLevel                    int // 0: debug  1: OK to log 2: should log
	electionTimeoutSecond          float64
	tickerSleepBaseTimeMillsecond  int
	heartbeatDurationMillSecond    int
	commitThreadIntervalMillsecond int
	lastGetLockTS                  time.Time
}

func (rf *Raft) requireLock(lock_name string) {
	rf.mu.Lock()
	rf.lastGetLockTS = time.Now()
	rf.advancedLog("requireLock", fmt.Sprintf("%s", lock_name), 0)
}

func (rf *Raft) releaseLock(lock_name string) {
	rf.advancedLog("releaseLock", fmt.Sprintf("%s, holding second %f", lock_name, time.Since(rf.lastGetLockTS).Seconds()), 0)
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.roleStatus == 2)
	rf.advancedLog("GetState", fmt.Sprintf("Role status : %d", rf.roleStatus), 0)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

// AppendEntries
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Reason  string
}

func (rf *Raft) checkIfIdxTermEqual(idx int, term int) bool {
	if idx == 0 {
		return true
	}
	if idx > len(rf.log) {
		return false
	}
	return rf.log[idx-1].Term == term
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.advancedLog("AppendEntries", fmt.Sprintf("Leader: %d\tLeaderTerm: %d", args.LeaderId, args.Term), -1)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Reason = "Term fail"
		return
	}

	rf.roleStatus = 0
	rf.refreshElectionTimeStamp()
	rf.currentTerm = args.Term

	// check if match log first, if success, update commit!
	idxMatch := rf.checkIfIdxTermEqual(args.PrevLogIndex, args.Term)

	rf.advancedLog("MatchCheck", fmt.Sprintf("\ncurrentLog: %+v\n appendDetail:%+v\n result:%t", rf.log, args, idxMatch), 3)
	if !idxMatch {
		reply.Success = false
		reply.Reason = "last IDX & term doesn't match"
		return
	}

	rf.log = rf.log[:args.PrevLogIndex]
	rf.log = append(rf.log, args.Entries...)

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
	}

	reply.Success = true
}

// Send appendEntries/heartbeat to server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) getLastLogIdxAndTerm() (int, int) {
	if len(rf.log) == 0 {
		return 0, 0
	}
	return len(rf.log), rf.log[len(rf.log)-1].Term
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term = rf.currentTerm
	isLeader = rf.roleStatus == 2

	if isLeader {
		index = len(rf.log) + 1
		rf.log = append(rf.log, LogEntry{command, term})
		rf.matchIndex[rf.me] = len(rf.log)
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartBeatToFollowers() {
	rf.refreshElectionTimeStamp()
	curLen := len(rf.log)
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(idx int) {
			appendEntriesArgs := AppendEntriesArgs{}
			appendEntriesArgs.LeaderId = rf.me
			appendEntriesArgs.Term = rf.currentTerm
			appendEntriesArgs.LeaderCommit = rf.commitIndex

			if rf.nextIndex[idx] == len(rf.log)+1 {
				appendEntriesArgs.Entries = []LogEntry{}
			} else {
				appendEntriesArgs.Entries = rf.log[rf.nextIndex[idx]-1:]
			}
			appendEntriesArgs.PrevLogIndex = rf.nextIndex[idx] - 1
			if appendEntriesArgs.PrevLogIndex == 0 {
				appendEntriesArgs.PrevLogTerm = 0
			} else {
				appendEntriesArgs.PrevLogTerm = rf.log[appendEntriesArgs.PrevLogIndex-1].Term
			}
			appendEntriesReply := AppendEntriesReply{}
			rf.sendAppendEntries(idx, &appendEntriesArgs, &appendEntriesReply)
			if appendEntriesReply.Term > rf.currentTerm {
				rf.roleStatus = 0
			}
			if appendEntriesReply.Success == false {
				/**
				Important optimize!
				Otherwise you would meet TestBackup2B: cannot reach agreement!
				It is because of the complexity!
				**/
				failPreTerm := appendEntriesArgs.PrevLogTerm
				for rf.nextIndex[idx] > 1 && rf.log[rf.nextIndex[idx]-2].Term == failPreTerm {
					rf.nextIndex[idx]--
				}
			} else {
				rf.matchIndex[idx] = curLen
				rf.nextIndex[idx] = curLen + 1
			}
		}(idx)
	}
}

func (rf *Raft) leaderCommitLogCheck() {
	peerCount := len(rf.matchIndex)
	nextIdxCommitCount := 0
	for i := 0; i < len(rf.matchIndex); i++ {
		if rf.matchIndex[i] > rf.commitIndex {
			nextIdxCommitCount++
		}
	}
	if nextIdxCommitCount > peerCount/2 {
		rf.commitIndex++
	}
}

func (rf *Raft) leaderSyncInfoCheck() {
	for rf.killed() == false {
		if rf.roleStatus == 2 {
			rf.sendHeartBeatToFollowers()

			rf.leaderCommitLogCheck()
		}

		time.Sleep(time.Duration(rf.heartbeatDurationMillSecond) * time.Millisecond)
	}
}

func (rf *Raft) checkIfNeedToApplyLogToMachine() {
	if rf.commitIndex > rf.lastApplied && len(rf.log) >= rf.lastApplied {
		rf.sendOneLogToApply()
	}
}

func (rf *Raft) commitLogToStateMachine() {
	for rf.killed() == false {
		rf.checkIfNeedToApplyLogToMachine()
		time.Sleep(time.Duration(rf.commitThreadIntervalMillsecond) * time.Millisecond)
	}
}

func (rf *Raft) sendOneLogToApply() {
	msg := ApplyMsg{}
	msg.Command = rf.log[rf.lastApplied].Command
	msg.CommandIndex = rf.lastApplied + 1
	msg.CommandValid = true
	rf.applyCh <- msg
	rf.advancedLog("WantToLogApply", fmt.Sprintf("Log len %d,LastApp %d ,ApplyMsg: %+v. ", len(rf.log), rf.lastApplied, msg), 4)
	rf.lastApplied++
	rf.advancedLog("LogApply", fmt.Sprintf("ApplyMsg: %+v. ", msg), 0)
}

func (rf *Raft) advancedLog(action string, info string, level int) {
	if level < rf.loggerLevel {
		return
	}
	fmt.Printf("%s\t%s\tServer: %d\t curTerm: %d\t%s\n", time.Now().Format("04:05.0000"), action, rf.me, rf.currentTerm, info)
}

//
// the service or tester wants to creaclearte a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.loggerLevel = 2
	rf.currentTerm = 0
	rf.roleStatus = 0
	rf.voteFor = -1
	rf.refreshElectionTimeStamp()
	rf.updateRandomElectionTimeoutSecond()

	rf.tickerSleepBaseTimeMillsecond = 10
	rf.heartbeatDurationMillSecond = 100

	rf.commitThreadIntervalMillsecond = 100

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// go heartbeat
	go rf.leaderSyncInfoCheck()

	// go commit log
	go rf.commitLogToStateMachine()

	return rf
}
