package raft

import (
	"bytes"

	"6.824/labgob"
)

type PersistentStruct struct {
	Log     []LogEntry
	LogLen  int
	VoteFor int
	Term    int
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	persistentStruct := PersistentStruct{
		Log:     rf.log,
		Term:    rf.currentTerm,
		VoteFor: rf.voteFor,
		LogLen:  rf.logLen,
	}
	e.Encode(persistentStruct)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var persistentStruct PersistentStruct
	d.Decode(&persistentStruct)
	//fmt.Printf("---- %+v \n", persistentStruct)

	if persistentStruct.Log != nil {
		rf.currentTerm = persistentStruct.Term
		rf.voteFor = persistentStruct.VoteFor
		rf.log = persistentStruct.Log
		rf.logLen = persistentStruct.LogLen
	} else {
		rf.currentTerm = 0
		rf.voteFor = -1
		rf.log = make([]LogEntry, 0)
		rf.logLen = 0
	}
}
