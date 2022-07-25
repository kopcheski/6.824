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
	logLen        int
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
	rf.requireLock("GetState")
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.roleStatus == 2)
	rf.advancedLog("GetState", fmt.Sprintf("Role status : %d", rf.roleStatus), 0)
	rf.releaseLock("GetState")
	return term, isleader
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
	if rf.logLen == 0 {
		return 0, 0
	}
	return rf.logLen, rf.log[rf.logLen-1].Term
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
	rf.heartbeatDurationMillSecond = 20

	rf.commitThreadIntervalMillsecond = 10

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
