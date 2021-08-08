package  raft

import (
	"github.com/VikasSherawat/raft/labrpc"
	"github.com/VikasSherawat/raft/storage"
	"math/rand"
	"sync"
	"time"
)

func Make(peers []*labrpc.ClientEnd, me int,
	persister *storage.Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// Your initialization code here (2A, 2B, 2C).

	//initialize volatile fields:
	//Volatile state on all servers:
	rf.commitIndex = 0
	rf.lastApplied = rf.log.LastIncludedIndex

	//added by me
	rf.state = follower
	rf.applyCh = applyCh

	//added in lab2B
	rf.newLogCome = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.heartbeatTimerTerminateChannel = make(chan bool)

	rand.Seed(int64(rf.me))
	// start ticker goroutine to start elections
	rf.timerLock.Lock()
	rf.timer = time.NewTimer(time.Duration(rand.Int())%electionTimeoutInterval + electionTimeoutStart)
	rf.timerLock.Unlock()
	go rf.ticker()
	//start the applier to send messages to applyCh
	go rf.applier()
	DPrintf("storage %d started----------------\n", rf.me)

	return rf
}