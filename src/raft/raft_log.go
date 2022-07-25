package raft

import (
	"fmt"
	"time"
)

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
	Term          int
	Success       bool
	ConflictIndex int
	Received      string
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.requireLock("AppendEntries")
	rf.advancedLog("AppendEntries", fmt.Sprintf("Leader: %d\tLeaderTerm: %d", args.LeaderId, args.Term), -1)
	reply.Term = rf.currentTerm
	reply.Received = "OK"

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Received = "False"
		rf.releaseLock("AppendEntries")
		return
	}

	rf.roleStatus = 0
	rf.refreshElectionTimeStamp()
	rf.currentTerm = args.Term

	// check if match log first, if success, update commit!
	idxMatch, conflictIndex := rf.checkIfIdxTermEqualAndConflitInfo(args.PrevLogIndex, args.Term)

	rf.advancedLog("MatchCheck", fmt.Sprintf("\ncurrentLog: %+v\n appendDetail:%+v\n result:%t", rf.log, args, idxMatch), 1)
	if !idxMatch {
		reply.Success = false
		reply.ConflictIndex = conflictIndex
		rf.releaseLock("AppendEntries")
		return
	}

	// optimize, not append all. but check and just append we need!
	argsEntriesLen := len(args.Entries)
	index := 0
	originLen := rf.logLen
	for ; index < argsEntriesLen; index++ {
		logIdx := args.PrevLogIndex + index
		if originLen <= logIdx {
			break
		}
		if rf.log[logIdx].Term != args.Entries[index].Term {
			rf.log[logIdx] = args.Entries[index]
		}
	}
	if argsEntriesLen > 0 {
		rf.advancedLog("Rewrite Log", fmt.Sprintf("Rewrite count: %d / %d.", index, len(args.Entries)), 10)
	}
	if index != argsEntriesLen {
		rf.log = rf.log[:args.PrevLogIndex+index]
		rf.log = append(rf.log, args.Entries[index:]...)
	}
	rf.logLen = args.PrevLogIndex + argsEntriesLen
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
	}
	reply.Success = true
	rf.persist()
	rf.releaseLock("AppendEntries")
	return
}

// Send appendEntries/heartbeat to server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) checkIfIdxTermEqualAndConflitInfo(prevIdx int, prevTerm int) (bool, int) {
	conflictIdx := 1
	if prevIdx == 0 {
		return true, conflictIdx
	}
	if prevIdx > rf.logLen {
		conflictIdx = rf.logLen + 1
		return false, conflictIdx
	}
	if rf.log[prevIdx-1].Term == prevTerm {
		return true, conflictIdx
	} else {
		conflictTerm := rf.log[prevIdx-1].Term
		for prevIdx >= 1 && rf.log[prevIdx-1].Term == conflictTerm {
			prevIdx--
		}
		conflictIdx = prevIdx + 1
		return false, conflictIdx
	}
}

func (rf *Raft) sendHeartBeatToFollowers() {
	rf.refreshElectionTimeStamp()
	curLen := rf.logLen
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(idx int) {
			rf.requireLock("sendHeartBeatInside1")
			appendEntriesArgs := AppendEntriesArgs{}
			appendEntriesArgs.LeaderId = rf.me
			appendEntriesArgs.Term = rf.currentTerm
			appendEntriesArgs.LeaderCommit = rf.commitIndex

			if rf.nextIndex[idx] == rf.logLen+1 {
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
			rf.releaseLock("sendHeartBeatInside1")

			rf.sendAppendEntries(idx, &appendEntriesArgs, &appendEntriesReply)

			rf.requireLock("sendHeartBeatInside2")

			if appendEntriesReply.Term > rf.currentTerm {
				rf.roleStatus = 0
				rf.releaseLock("sendHeartBeatInside2")
				return
			}

			if appendEntriesReply.Received != "OK" {
				rf.releaseLock("sendHeartBeatInside2")
				return
			}
			if appendEntriesReply.Success == false {
				/**
				Important optimize!
				Otherwise you would meet TestBackup2B: cannot reach agreement!
				It is because of the complexity!
				**/
				if appendEntriesReply.ConflictIndex < rf.nextIndex[idx] {
					oldIdx := rf.nextIndex[idx]
					rf.nextIndex[idx] = appendEntriesReply.ConflictIndex
					rf.advancedLog("ConflitIdxJumpIdx", fmt.Sprintf("follower: %d, From : %d to %d ", idx, oldIdx, rf.nextIndex[idx]), 10)
				}
			} else {
				rf.matchIndex[idx] = curLen
				rf.nextIndex[idx] = curLen + 1
			}
			rf.releaseLock("sendHeartBeatInside2")
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
		rf.persist()
	}
}

func (rf *Raft) leaderSyncInfoCheck() {
	for rf.killed() == false {
		rf.requireLock("leaderSyncInfoCheck")
		if rf.roleStatus == 2 {
			rf.sendHeartBeatToFollowers()

			rf.leaderCommitLogCheck()
		}
		rf.releaseLock("leaderSyncInfoCheck")
		time.Sleep(time.Duration(rf.heartbeatDurationMillSecond) * time.Millisecond)
	}
}

func (rf *Raft) checkIfNeedToApplyLogToMachine() {
	rf.requireLock("checkIfNeedToApplyLogToMachine")
	count := 0
	for rf.commitIndex > rf.lastApplied && rf.logLen >= rf.lastApplied {
		rf.sendOneLogToApply()
		count++
	}
	rf.releaseLock("checkIfNeedToApplyLogToMachine")
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
	rf.advancedLog("WantToLogApply", fmt.Sprintf("Log len %d,LastApp %d ,ApplyMsg: %+v. ", rf.logLen, rf.lastApplied, msg), 4)
	rf.lastApplied++
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.requireLock("Start")
	term = rf.currentTerm
	isLeader = rf.roleStatus == 2

	if isLeader {
		index = rf.logLen + 1
		rf.log = append(rf.log, LogEntry{command, term})
		rf.logLen++
		rf.matchIndex[rf.me] = rf.logLen
	}
	rf.persist()
	rf.releaseLock("Start")
	return index, term, isLeader
}
