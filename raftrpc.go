package raft

func (rf *Raft) sendRequestVoteRPC(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//VERY IMPORTANT:
	//this function may wait for quite a long time to return.
	//we sometimes need to resend a heartbeat before it return.
	//so this function must be executed in a seperate goroutine to achieve this.
	//see rf.leader()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) checkRequestVote(reply RequestVoteReply, currentTerm int) bool {
	if reply.Term > rf.currentTerm {
		//we are outdated
		rf.currentTerm = reply.Term
		rf.persist()
		rf.State = FOLLOWER

		return false
	}
	return true
}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}