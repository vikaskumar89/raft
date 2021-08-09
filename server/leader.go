package server

import (
	"fmt"
	"time"
)

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

	currentTerm := rf.CurrentTerm

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
			go rf.sendAppendEntries(args, currentTerm, server)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for !rf.killed() {
				if rf.CurrentTerm != currentTerm && rf.State != LEADER {
					return
				}

				//each loop: send all available log entries available.

				//if LEADER is idle, then it should wait until new log entry comes or timer fire.
				for !rf.killed() && rf.NextIndex[server] > rf.Log.lastIndex() {
					if rf.CurrentTerm != currentTerm && rf.State != LEADER {
						return
					}
					//if it wakes up and find still idle,
					//then it must be woken up by heartBeatChannel,
					//should send heartbeat.
					prevLogIndex := rf.NextIndex[server] - 1 //IMPORTANT: prevLogIndex, not lastLogEntryIndex!(though equal here.)
					prevLogTerm := rf.Log.index(prevLogIndex).Term
					args := AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: []LogEntry{}, LeaderCommit: rf.CommitIndex}
					DPrintf("LEADER %d send heartbeat to %d\n", rf.me, server)
					go rf.sendAppendEntries(args, currentTerm, server)
					rf.newLogCome.Wait()
				}

				//not idle
				//still in rf.mu.Lock()
				if !rf.killed() && rf.NextIndex[server] <= rf.Log.lastIndex() {
					if rf.CurrentTerm != currentTerm && rf.State != LEADER {
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
						go rf.sendAppendEntries(args, currentTerm, server)
					}

					//the following code is sending too fast. This will cause congestion and slow down
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

func (rf *Raft) sendAppendEntries(args AppendEntriesArgs, currentTerm int, server int) {
	//fmt.Printf("server %d begin sending to server %d, with %d log entrys in args \n", rf.me, server, len(args.Entries))
	//fmt.Printf("curently %d goroutines.\n", runtime.NumGoroutine())
	reply := AppendEntriesReply{}
	//start := time.Now()

	ok := rf.sendAppendEntriesRPC(server, &args, &reply)

	//elapsed := time.Since(start)
	//log.Printf("server %d received reply from server %d, took %s", rf.me, server, elapsed)
	if ok && !rf.killed() {
		DPrintf("leader %d receive from %d\n", rf.me, server)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//defer fmt.Printf("server %d sender exit, ok==true\n", rf.me)
		if currentTerm != rf.CurrentTerm || rf.State != LEADER || rf.checkAppendEntriesReply(reply, currentTerm) == false {
			return
		}
		rf.appendEntriesReplyHandle(args, reply, currentTerm, server)
	}
	//fmt.Printf("server %d sender exit, ok==false\n", rf.me)
}

func (rf *Raft) appendEntriesReplyHandle(args AppendEntriesArgs, reply AppendEntriesReply, currentTerm int, server int) {
	//must be holding the lock
	if reply.Success {
		rf.NextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
		majorityMatchIndex := findKthLargest(rf.MatchIndex, len(rf.peers)/2+1)
		if majorityMatchIndex > rf.CommitIndex && rf.Log.index(majorityMatchIndex).Term == rf.CurrentTerm {
			rf.CommitIndex = majorityMatchIndex
			DPrintf("leader MatchIndex: %v\n", rf.MatchIndex)
			if rf.CommitIndex > rf.LastApplied {
				rf.applyCond.Signal()
			}
		}

	} else {
		//implement optimization mentioned in the paper
		tmp := rf.NextIndex[server]
		if reply.ConflictTermFirstIndex == -1 {
			//reply to outdated request
			return
		}
		if reply.ConflictEntryTerm == -1 {
			//follower's log shorter than rf.NextIndex[server]
			rf.NextIndex[server] = reply.ConflictTermFirstIndex
		} else if reply.ConflictEntryTerm < args.PrevLogTerm {
			//go back to last term of the leader
			for rf.NextIndex[server] > rf.Log.LastIncludedIndex+1 && rf.Log.index(rf.NextIndex[server]-1).Term == args.PrevLogTerm {
				rf.NextIndex[server]--
			}
			if rf.NextIndex[server] != 1 && rf.NextIndex[server] == rf.Log.LastIncludedIndex+1 && rf.Log.LastIncludedTerm == args.PrevLogTerm {
				//special case:
				//we are hitting the snapshot.
				rf.NextIndex[server]--
			}
		} else {
			//reply.ConflictEntryTerm > args.PrevLogTerm
			//go back to last term of the follower
			rf.NextIndex[server] = reply.ConflictTermFirstIndex
		}
		DPrintf("rf.NextIndex[server] decreased: %d\n", rf.NextIndex[server])

		if rf.NextIndex[server] < 1 {
			fmt.Printf("ERROR: rf.NextIndex[server] < 1 (=%d)\n", tmp-rf.NextIndex[server])
		}
	}
}

func (rf *Raft) checkAppendEntriesReply(reply AppendEntriesReply, currentTerm int) bool {
	if reply.Term > rf.CurrentTerm {
		//we are outdated
		rf.heartbeatTimerTerminateChannel <- true
		rf.State = FOLLOWER
		rf.CurrentTerm = reply.Term
		rf.persist()
		rf.resetTimer() //restart ticker
		return false
	}
	return true
}

func (rf *Raft) sender_snapshot(args InstallSnapshotArgs, currentTerm int, server int) {
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshotRPC(server, &args, &reply)
	if ok && !rf.killed() {
		DPrintf("leader %d receive sendInstallSnapshot reply from %d\n", rf.me, server)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//defer fmt.Printf("server %d sender exit, ok==true\n", rf.me)
		if currentTerm != rf.CurrentTerm || rf.State != LEADER || rf.checkInstallSnapshotReply(reply) == false {
			return
		}
		rf.NextIndex[server] = args.LastIncludedIndex + 1
	}
}

func (rf *Raft) checkInstallSnapshotReply(reply InstallSnapshotReply) bool {
	if reply.Term > rf.CurrentTerm {
		//we are outdated
		rf.heartbeatTimerTerminateChannel <- true
		rf.State = FOLLOWER
		rf.CurrentTerm = reply.Term
		rf.persist()
		rf.resetTimer() //restart ticker
		return false
	}
	return true
}