package server

func (rf *Raft) candidate() {
	rf.mu.Lock()
	if rf.State == LEADER { //check if leader
		rf.mu.Unlock()
		return
	}

	rf.CurrentTerm++
	rf.State = CANDIDATE

	//fmt.Printf("elect of process %d, term is %d\n", rf.me, rf.CurrentTerm)
	currentTerm := rf.CurrentTerm
	args := RequestVoteArgs{currentTerm, rf.me, rf.Log.lastIndex(), rf.Log.index(rf.Log.lastIndex()).Term}
	rf.VotedFor = rf.me //vote for itself
	rf.persist()
	rf.VoteCount = 1
	rf.mu.Unlock()

	//start len(rf.peers) subgoroutines to handle leader job seperately.
	rf.candidateProcess(args, currentTerm)
}

func (rf *Raft) candidateProcess(args RequestVoteArgs, currentTerm int) {
	//request vote in parallel
	for server := range rf.peers {
		if server == rf.me { //do not send to myself
			continue
		}

		go func(server int) {
			//send only once is enough to satisfy raft rules.
			reply := RequestVoteReply{}
			DPrintf("server %d send requestvote to %d\n", rf.me, server)
			ok := rf.sendRequestVoteRPC(server, &args, &reply)
			if ok {
				DPrintf("server %d receive requestvote from %d\n", rf.me, server)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if currentTerm != rf.CurrentTerm || rf.State != CANDIDATE || rf.VoteCount >= len(rf.peers)/2+1 || rf.checkRequestVote(reply, currentTerm) == false {
					return
				}
				DPrintf("server %d receive requestvote from %d: passed check\n", rf.me, server)
				if reply.VoteGranted {
					DPrintf("server %d receive requestvote from %d: vote granted\n", rf.me, server)
					rf.VoteCount++
					if rf.VoteCount == len(rf.peers)/2+1 {
						go rf.leader()
					}
				}
			}
		}(server)
	}
}

func (rf *Raft) checkRequestVote(reply RequestVoteReply, currentTerm int) bool {
	if reply.Term > rf.CurrentTerm {
		//we are outdated
		rf.CurrentTerm = reply.Term
		rf.persist()
		rf.State = FOLLOWER

		return false
	}
	return true
}