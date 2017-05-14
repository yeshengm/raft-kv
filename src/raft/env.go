package raft

import (
	"fmt"
	"rpc"
	"runtime"
	"sync"
	"testing"
)

// create an environment to run simulations of raft

type env struct {
	sync.Mutex
	t         *testing.T
	net       *rpc.Network
	n         int   // number of raft peers
	done      int32 // tell threads to die
	rafts     []*Raft
	applyErr  []string
	connected []bool
	saved     []*Persister
	endNames  [][]string
	logs      []map[int]int
}

var ncpu_once sync.Once

// creates an environment to run raft
func makeEnv(t *testing.T, n int, unreliable bool) *env {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Println("warning: only one CPU, which may conceal locking bugs")
		}
	})
	runtime.GOMAXPROCS(4)
	cfg := &env{
		t:         t,
		net:       rpc.MakeNetwork(),
		n:         n,
		done:      0,
		applyErr:  make([]string, n),
		rafts:     make([]*Raft, n),
		connected: make([]bool, n),
		saved:     make([]*Persister, n),
		endNames:  make([]string, n),
		logs:      make([]map[int]int, n),
	}
	// TODO add raft peers
}

func (e *env) setunreliable(yes bool)
