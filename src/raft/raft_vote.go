package raft

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.requireLock("RequestVote")
	defer rf.releaseLock("RequestVote")
	reply.Term = rf.currentTerm
	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	rf.currentTerm = args.Term

	// log at least up to date
	lastLogIdx, lastLogTerm := rf.getLastLogIdxAndTerm()
	atLeastUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		atLeastUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx {
		atLeastUpToDate = true
	}

	rf.advancedLog("RequestVoteAtleast", "atLeast "+strconv.FormatBool(atLeastUpToDate), 2)
	if !atLeastUpToDate {
		reply.VoteGranted = false
		return
	}

	rf.refreshElectionTimeStamp()

	rf.voteFor = args.CandidateId
	rf.roleStatus = 0
	// TODO: check here
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.advancedLog("ElectionVote", "Vote for "+strconv.Itoa(rf.voteFor), 2)
}

func (rf *Raft) updateRandomElectionTimeoutSecond() {
	rf.electionTimeoutSecond = 0.15 + float64(rand.Intn(1500))/1000
}

func (rf *Raft) refreshElectionTimeStamp() {
	rf.timeoutLastTS = time.Now()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.requireLock("ticker")
		if rf.roleStatus != 2 && time.Since(rf.timeoutLastTS).Seconds() > float64(rf.electionTimeoutSecond) {
			rf.updateRandomElectionTimeoutSecond()

			// start a election
			rf.currentTerm += 1
			rf.refreshElectionTimeStamp()
			rf.voteFor = rf.me
			rf.roleStatus = 1

			rf.advancedLog("ElectionStart", "become the candidate", 2)

			requestVoteArgs := RequestVoteArgs{}
			requestVoteArgs.CandidateId = rf.me
			requestVoteArgs.Term = rf.currentTerm
			requestVoteArgs.LastLogIndex = rf.logLen
			if requestVoteArgs.LastLogIndex == 0 {
				requestVoteArgs.LastLogTerm = 0
			} else {
				requestVoteArgs.LastLogTerm = rf.log[requestVoteArgs.LastLogIndex-1].Term
			}

			gotCount := 1
			countMutex := sync.Mutex{}

			haveBecomeLeader := false
			for idx := range rf.peers {
				if idx == rf.me {
					continue
				}

				go func(idx int) {

					requestVoteReply := RequestVoteReply{}

					rf.sendRequestVote(idx, &requestVoteArgs, &requestVoteReply)
					rf.requireLock("tickerInside")
					defer rf.releaseLock("tickerInside")
					if requestVoteReply.Term > rf.currentTerm {
						rf.roleStatus = 0
						return
					}

					if requestVoteReply.Term == rf.currentTerm && requestVoteReply.VoteGranted {
						countMutex.Lock()
						gotCount++
						countMutex.Unlock()
						if gotCount > len(rf.peers)/2 {
							if rf.roleStatus != 2 && !haveBecomeLeader {
								haveBecomeLeader = true
								rf.roleStatus = 2
								rf.advancedLog("ElectionEnd", fmt.Sprintf("become the leader. vote count: %d. peers count: %d", gotCount, len(rf.peers)), 2)

								// reinit_client_index
								rf.matchIndex = make([]int, len(rf.peers))
								rf.nextIndex = make([]int, len(rf.peers))
								nextIdx := rf.logLen + 1
								for i := 0; i < len(rf.peers); i++ {
									rf.matchIndex[i] = 0
									rf.nextIndex[i] = nextIdx
								}
							}
						}
					}
				}(idx)
			}
		}
		rf.releaseLock("ticker")
		time.Sleep(time.Duration(rf.tickerSleepBaseTimeMillsecond) * time.Millisecond)
	}
}
