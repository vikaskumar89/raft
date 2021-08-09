package server

import "time"

const (
	FOLLOWER                serverState = iota
	CANDIDATE               serverState = iota
	LEADER                  serverState = iota
	electionTimeoutStart                = 600 * time.Millisecond //400
	electionTimeoutInterval             = 100 * time.Millisecond
	heartbeatInterval                   = 200 * time.Millisecond //100
)
