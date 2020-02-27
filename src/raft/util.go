package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Min(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}

func Max(x int, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}