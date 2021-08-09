package raft

import "time"

func (rf *Raft) leader() {
	DPrintf("leader:%d\n", rf.me)
	go func() { rf.ApplyCh <- ApplyMsg{} }()
	//fmt.Printf("NEW LEADER ELECTED\n")

	rf.mu.Lock()
	rf.State = LEADER
	//Volatile state on leaders:
	rf.NextIndex = make([]int, len(rf.peers))
	for server := range rf.NextIndex {
		rf.NextIndex[server] = rf.Log.lastIndex() + 1
	}
	rf.MatchIndex = make([]int, len(rf.peers)) //initialized to 0.

	currentTerm := rf.currentTerm

	//set a timer for all subgoroutines created by rf.leaderProcess(to send heartbeat).
	heartbeatTimer := time.NewTimer(heartbeatInterval)
	go func() {
		for {
			select {
			case <-heartbeatTimer.C:
				rf.newLogCome.Broadcast()
				heartbeatTimer.Reset(heartbeatInterval)
			case <-rf.heartbeatTimerTerminateChannel:
				rf.newLogCome.Broadcast()
				return
			}
		}
	}()

	//start len(rf.peers) subgoroutines to handle leader job seperately.
	rf.leaderProcess(currentTerm)
	rf.mu.Unlock()

}
