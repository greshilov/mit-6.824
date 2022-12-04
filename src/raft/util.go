package raft

import (
	"log"
	"sort"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func getMajority(array []int) int {
	m := make([]int, len(array))
	copy(m, array)

	sort.Ints(m)

	return m[len(m)/2]
}

type Empty struct{}
