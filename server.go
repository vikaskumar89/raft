package raft

import (
	"bytes"
	"fmt"
	"github.com/VikasSherawat/raft/labgob"
	"math/rand"
	"time"
)

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
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.Log.Entries = make([]LogEntry, 0) //empty. (first index is one)
		rf.Log.LastIncludedIndex = 0
		rf.Log.LastIncludedTerm = -1
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log LogType
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("server %d readPersist: decode error!", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.Log = log
	}
}
func (rf *Raft) ticker() {
	//fmt.Printf("ticker %d start\n", rf.me)
	//ticker never exit.
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//my implementation does not use timer.Sleep()
		//All used time.Timer().

		<-rf.timer.C
		if rf.killed() {
			break
		}

		//timer fired
		//start election
		//must do election in a seperate thread
		//since election and timer has to run concurrently.
		go rf.candidate()

		rf.timerLock.Lock()
		duration := time.Duration(rand.Int())%electionTimeoutInterval + electionTimeoutStart
		rf.timer.Reset(duration)
		rf.timerLock.Unlock()
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.CommitIndex > rf.LastApplied {
			rf.LastApplied++
			DPrintf("server %d admit %d.\n\n", rf.me, rf.LastApplied)
			msg := ApplyMsg{CommandValid: true, Command: rf.Log.index(rf.LastApplied).Command, CommandIndex: rf.LastApplied}
			rf.mu.Unlock()
			//IMPORTANT: must **not** holding the lock while sending to applyCh.
			//OR will cause deadlock(In 2D, since Snapshot() need to hold rf.mu)!
			rf.ApplyCh <- msg
			rf.mu.Lock()
			DPrintf("admitted\n")
		} else {
			rf.applyCond.Wait()
		}
	}
	close(rf.ApplyCh)
}

func (l *LogType) index(index int) LogEntry {
	if index > l.LastIncludedIndex+len(l.Entries) {
		panic("ERROR: index greater than log length!\n")
	} else if index < l.LastIncludedIndex {
		panic("ERROR: index smaller than log snapshot!\n")
	} else if index == l.LastIncludedIndex {
		//fmt.Printf("WARNING: index == l.LastIncludedIndex\n")
		return LogEntry{Term: l.LastIncludedTerm, Command: nil}
	}
	return l.Entries[index-l.LastIncludedIndex-1]
}
func (l *LogType) lastIndex() int {
	return l.LastIncludedIndex + len(l.Entries)
}