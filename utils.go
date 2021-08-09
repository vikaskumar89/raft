package raft

import (
	"container/heap"
	"log"
	"sync/atomic"
)

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

const Debug = false

func DPrintf(format string, a ...interface{})  {
	if Debug {
		log.Printf(format, a...)
	}
}

type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func findKthLargest(nums []int, k int) int {
	h := &IntHeap{}
	heap.Init(h)
	for _, item := range nums {
		heap.Push(h, item)
		if h.Len() > k {
			heap.Pop(h)
		}
	}
	return (*h)[0]
}