package raft

import "time"

const (
	FOLLOWER                serverState   = iota
	CANDIDATE               serverState   = iota
	LEADER                  serverState   = iota
	electionTimeoutStart    time.Duration = 600 * time.Millisecond //400
	electionTimeoutInterval time.Duration = 100 * time.Millisecond
	heartbeatInterval       time.Duration = 200 * time.Millisecond //100
)

