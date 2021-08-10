# raft
Implementation of Raft servers as described in Raft Extended Paper. 

## It covers following features.
1. Leader Election
2. Log Replication
3. Log Compaction

## Following RPCs are implemented as describe in figure 2 of the Paper.
1. RequestVote
2. AppendEntries
3. InstallSnapshot

This implementation is tested locally by connecting a number of raft servers in a cluster and by simulating server failures, delayed and unreliable network

