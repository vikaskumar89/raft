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

func (rf *Raft) leaderProcess(currentTerm int) {
	//transient function.
	//start many goroutines to send heartbeat in parallel
	for server := range rf.peers {
		if server == rf.me { //do not send to myself
			continue
		}
		prevLogIndex := rf.NextIndex[server] - 1 //IMPORTANT: prevLogIndex, not lastLogEntryIndex!
		prevLogTerm := rf.Log.index(prevLogIndex).Term
		args := AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: []LogEntry{}, LeaderCommit: rf.CommitIndex}
		go func(server int) { //use seperate goroutines to send messages: can set independent timers.
			//initial heartbeat.
			DPrintf("leader %d send heartbeat to %d\n", rf.me, server)
			go rf.sender(args, currentTerm, server)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for !rf.killed() {
				if rf.currentTerm != currentTerm && rf.state != leader {
					return
				}

				//each loop: send all available log entries available.

				//if leader is idle, then it should wait until new log entry comes or timer fire.
				for !rf.killed() && rf.NextIndex[server] > rf.log.lastIndex() {
					if rf.currentTerm != currentTerm && rf.state != leader {
						return
					}
					//if it wakes up and find still idle,
					//then it must be woken up by heartBeatChannel,
					//should send heartbeat.
					prevLogIndex := rf.NextIndex[server] - 1 //IMPORTANT: prevLogIndex, not lastLogEntryIndex!(though equal here.)
					prevLogTerm := rf.log.index(prevLogIndex).Term
					args := AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: []LogEntry{}, LeaderCommit: rf.CommitIndex}
					DPrintf("leader %d send heartbeat to %d\n", rf.me, server)
					go rf.sender(args, currentTerm, server)
					rf.newLogCome.Wait()
				}

				//not idle
				//still in rf.mu.Lock()
				if !rf.killed() && rf.NextIndex[server] <= rf.Log.lastIndex() {
					if rf.currentTerm != currentTerm && rf.state != leader {
						return
					}
					prevLogIndex = rf.NextIndex[server] - 1
					if prevLogIndex < rf.Log.LastIncludedIndex {
						//should send snapshot
						args := InstallSnapshotArgs{Term: currentTerm, LeaderID: rf.me, LastIncludedIndex: rf.Log.LastIncludedIndex, LastIncludedTerm: rf.Log.LastIncludedTerm, Data: rf.snapshot}
						go rf.sender_snapshot(args, currentTerm, server)
					} else {
						prevLogTerm := rf.Log.index(prevLogIndex).Term
						//should send normal appendentries RPC.
						args := AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: append([]LogEntry(nil), rf.Log.Entries[prevLogIndex-rf.Log.LastIncludedIndex:]...), LeaderCommit: rf.CommitIndex}
						go rf.sender(args, currentTerm, server)
					}

					//the following code is sending too fast. This will cause congestion and slow down execution in lab3A.

					/*rf.mu.Unlock()
					time.Sleep(heartbeatInterval / 5) //wait for rf.sender to get reply and process it
					rf.mu.Lock()*/

					//so I changed the code to the following line:
					rf.newLogCome.Wait()
				}
			}
		}(server)
	}
}
