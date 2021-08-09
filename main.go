package  raft

import (
	"github.com/VikasSherawat/raft/labrpc"
	"github.com/VikasSherawat/raft/server"
	"github.com/VikasSherawat/raft/storage"
)
func Make(peers []*labrpc.ClientEnd, me int,
	persister *storage.Persister, applyCh chan server.ApplyMsg) *server.Raft {
	return server.Make(peers, me, persister, applyCh)
}