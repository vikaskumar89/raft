package server

import "fmt"

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//IMPORTANT
	//RPC handler do not change state itself.
	//It only send Abort messages to leader() or elect().
	//changing state is done by them
	//otherwise leader() or candidate() may be confused about their state.

	//different from AppendEntries: reset timer only when we grant the vote.
	rf.mu.Lock() //hold the lock during the whole process
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.CurrentTerm }()

	grantVote := func() {
		DPrintf("server %d vote for server %d\n", rf.me, args.CandidateID)
		rf.resetTimer() //reset timer
		rf.VotedFor = args.CandidateID
		rf.persist()
		reply.VoteGranted = true
	}

	switch {
	case args.Term < rf.CurrentTerm:
		//outdated request
		reply.VoteGranted = false

	case args.Term >= rf.CurrentTerm:
		if args.Term > rf.CurrentTerm {
			//we are outdated
			rf.CurrentTerm = args.Term

			if rf.State != FOLLOWER {
				rf.State = FOLLOWER
			}

			rf.VotedFor = -1
			rf.persist()
		}

		//normal
		if rf.VotedFor != -1 && rf.VotedFor != args.CandidateID { //already voted for others in this term
			reply.VoteGranted = false
			return
		}
		//check up-to-date
		switch {
		case args.LastLogTerm > rf.Log.index(rf.Log.lastIndex()).Term:
			grantVote()
		case args.LastLogTerm < rf.Log.index(rf.Log.lastIndex()).Term:
			reply.VoteGranted = false
		case args.LastLogTerm == rf.Log.index(rf.Log.lastIndex()).Term:
			if args.LastLogIndex >= rf.Log.lastIndex() {
				grantVote()
			} else {
				reply.VoteGranted = false
			}
		}
	}
	return
}
func (rf *Raft) sendRequestVoteRPC(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//VERY IMPORTANT:
	//this function may wait for quite a long time to return.
	//we sometimes need to resend a heartbeat before it return.
	//so this function must be executed in a seperate goroutine to achieve this.
	//see rf.LEADER()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//IMPORTANT
	//RPC handler do not change State itself.
	//It only send Abort messages to LEADER() or elect().
	//changing State is done by them
	//otherwise LEADER() or candidate() may be confused about their State.
	rf.mu.Lock() //hold the lock during entire handle process
	DPrintf("server %d receive AppendEntries from %d\n", rf.me, args.LeaderID)
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.CurrentTerm }()

	reply.ConflictEntryTerm = -1
	reply.ConflictTermFirstIndex = -1

	switch {
	case args.Term < rf.CurrentTerm:
		//outdated request
		DPrintf("server %d: false1, args.Term%d < rf.CurrentTerm%d\n", rf.me, args.Term, rf.CurrentTerm)
		reply.Success = false
		return

	case args.Term > rf.CurrentTerm:
		//we are outdated
		rf.CurrentTerm = args.Term
		rf.persist()

		if rf.State != FOLLOWER {
			rf.State = FOLLOWER
		}

	case args.Term == rf.CurrentTerm:
		//normal
		if rf.State == LEADER {
			fmt.Printf("ERROR! Another LEADER in current term?!") //impossible
		} else if rf.State == CANDIDATE {
			//fmt.Printf("Candidate %d abdicate!\n", rf.me)
			rf.State = FOLLOWER

		}
	}

	if args.PrevLogIndex < rf.Log.LastIncludedIndex {
		//outdated AppendEntries
		//only happen due to unreliable network
		reply.Success = false
		return
	}

	rf.resetTimer() //reset timer

	//lab 2b
	if args.PrevLogIndex > 0 && (args.PrevLogIndex > rf.Log.lastIndex() || rf.Log.index(args.PrevLogIndex).Term != args.PrevLogTerm) {
		//Reply false if Log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Success = false
		DPrintf("server %d: false2\n", rf.me)
		DPrintf("args.PrevLogIndex: %d,rf.Log.lastIndex(): %d\n", args.PrevLogIndex, rf.Log.lastIndex())

		if args.PrevLogIndex > rf.Log.lastIndex() {
			reply.ConflictTermFirstIndex = rf.Log.lastIndex() + 1
			reply.ConflictEntryTerm = -1
			return
		}

		reply.ConflictEntryTerm = rf.Log.index(args.PrevLogIndex).Term
		conflictTermFirstIndex := args.PrevLogIndex
		for conflictTermFirstIndex >= rf.Log.LastIncludedIndex && rf.Log.index(conflictTermFirstIndex).Term == reply.ConflictEntryTerm {
			conflictTermFirstIndex--
		}
		reply.ConflictTermFirstIndex = conflictTermFirstIndex + 1

		return
	}
	DPrintf("reply.Success = true on server %d\n", rf.me)
	DPrintf("len(args.Entries): %d\n", len(args.Entries))

	reply.Success = true

	var i int
	for i = 0; i < len(args.Entries); i++ {
		//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		//here can't write "for i =range(args.Entries)",
		//since it will not increment i when i==len(args.Entries)-1
		//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		//it spent me 2 hours here to debug.
		if args.PrevLogIndex+i+1 > rf.Log.lastIndex() {
			break
		}
		if args.Entries[i].Term == rf.Log.index(args.PrevLogIndex+i+1).Term {
			continue
		}
		//If an existing entry conflicts with a new one
		//(same index but different terms),
		//delete the existing entry and all that follow it
		rf.Log.Entries = rf.Log.Entries[:args.PrevLogIndex+i+1-rf.Log.LastIncludedIndex-1]
		rf.persist()
		break
	}

	for j := i; j < len(args.Entries); j++ {
		rf.Log.Entries = append(rf.Log.Entries, args.Entries[j])
		DPrintf("server %d append: %d", rf.me, rf.Log.lastIndex())
	}
	rf.persist()

	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, rf.Log.lastIndex()) //index of last new entry

		if rf.CommitIndex > rf.LastApplied {
			rf.applyCond.Signal()
		}
	}

}