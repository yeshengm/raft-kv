package rpc

import "sync"

// RPC API for raft
//
// MakeNetwork()
//		MakeEnd(endName)
//		AddServer(serverName, server)
//		DeleteServer(serverName)
//		Connect(endName, serverName)
//		Enable(endName, enabled)
//    Reliable(yes)

// Network maintains the status of simulated network
// and serves as an intermediate router for requests
type Network struct {
	sync.Mutex
	reliable    bool
	longDelays  bool
	reordering  bool
	ends        map[interface{}]*ClientEnd
	enabled     map[interface{}]bool
	servers     map[interface{}]*Server
	connections map[interface{}]interface{} // end->server map
	endCh       chan reqMsg
}
