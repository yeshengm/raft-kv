package rpc

import (
	"log"
	"sync"
)

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
	reliable    bool // network configs, by default reliable
	longDelays  bool
	reordering  bool
	ends        map[interface{}]*ClientEnd
	enabled     map[interface{}]bool
	servers     map[interface{}]*Server
	connections map[interface{}]interface{} // end->server map
	endCh       chan reqMsg                 // channel for all incoming reqs
}

// Network factory method
func MakeNetwork() *Network {
	// some fields are defaultly initialized
	net := &Network{
		reliable:    true,
		ends:        make(map[interface{}]*ClientEnd),
		enabled:     make(map[interface{}]bool),
		servers:     make(map[interface{}]*Server),
		connections: make(map[interface{}]interface{}),
		endCh:       make(chan reqMsg),
	}
	return net
}

// Network setters
func (net *Network) Reliable(b bool) {
	net.Lock()
	defer net.Unlock()
	net.reliable = b
}

func (net *Network) LongDelays(b bool) {
	net.Lock()
	defer net.Unlonk()
	net.longDelays = b
}

func (net *Network) Reordering(b bool) {
	net.Lock()
	defer net.Unlock()
	net.reordering = b
}

// create client ends/servers
func (net *Network) AddClientEnd(en interface{}) *ClientEnd {
	net.Lock()
	defer net.Unlock()

	if _, ok := net.ends[en]; ok {
		log.Fatalf("MakeClientEnd: %v end already exists", en)
	}
	e := &ClientEnd{
		endName: en,
		ch:      net.endCh,
	}
	net.ends[en] = e
	net.enabled[en] = false
	net.connections[en] = nil
	return e
}

func (net *Network) AddServer(sn interface{}, rs *Server) {
	net.Lock()
	defer net.Unlock()
	if _, ok := net.servers[sn]; ok {
		log.Fatalf("MakeServer: %v already exsits\n", sn)
	}
	net.servers[sn] = rs
}

func (net *Network) DeleteServer(sn interface{}) {
	net.Lock()
	defer net.Unlock()
	// no need to delete in map since this is a temp failure
	net.servers[sn] = nil
}

func (net *Network) Connect(en interface{}, sn interface{}) {
	net.Lock()
	defer net.Unlock()
	if _, ok := net.connections[en]; !ok {
		log.Fatalf("Connect: non-existing end %v\n", en)
	}
	// sn should be a server name or nil
	if _, ok := net.servers[sn]; !ok && sn != nil {
		log.Fatalf("Connect: non-exsiting server %v\n", sn)
	}
	net.connections[en] = sn
}

func (net *Network) Enable(en interface{}, b bool) {
	ru.Lock()
	defer ru.Unlock()
	if _, ok := net.ends[en]; !ok {
		log.Fatalf("Enable: non-existing end %v\n", en)
	}
	net.enabled[en] = b
}
