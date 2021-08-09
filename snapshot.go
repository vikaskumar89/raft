package raft

import (
	"bytes"
	"github.com/VikasSherawat/raft/labgob"
)

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.CommitIndex {
		//fmt.Printf("CondInstallSnapshot refused\n")
		return false
	}

	defer func() {
		rf.Log.LastIncludedIndex = lastIncludedIndex
		rf.Log.LastIncludedTerm = lastIncludedTerm
		rf.snapshot = snapshot
		rf.CommitIndex = lastIncludedIndex //IMPORTANT
		rf.LastApplied = lastIncludedIndex //IMPORTANT
		rf.persistStateAndSnapshot(snapshot)

	}()
	if lastIncludedIndex <= rf.Log.lastIndex() && rf.Log.index(lastIncludedIndex).Term == lastIncludedTerm {
		rf.Log.Entries = append([]LogEntry(nil), rf.Log.Entries[lastIncludedIndex-rf.Log.LastIncludedIndex:]...)
		return true
	}

	//discard the entire Log
	rf.Log.Entries = make([]LogEntry, 0)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d: Snapshot create\n", rf.me)
	if index <= rf.Log.LastIncludedIndex {
		//already created a snapshot
		return
	}
	rf.Log.Entries = append([]LogEntry(nil), rf.Log.Entries[index-rf.Log.LastIncludedIndex:]...)
	rf.Log.LastIncludedIndex = index
	rf.Log.LastIncludedTerm = rf.Log.index(index).Term
	rf.snapshot = snapshot
	rf.persistStateAndSnapshot(snapshot)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.Log)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, snapshot)
}
