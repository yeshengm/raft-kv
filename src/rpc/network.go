package rpc

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

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
	go func() {
		for req := range net.endCh {
			go net.ProcessRequest(req)
		}
	}()
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
	defer net.Unlock()
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
		log.Fatalf("MakeClientEnd: %v already exists", en)
	}
	e := &ClientEnd{
		endName: en,
		reqCh:   net.endCh,
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
	net.Lock()
	defer net.Unlock()
	if _, ok := net.ends[en]; !ok {
		log.Fatalf("Enable: non-existing end %v\n", en)
	}
	net.enabled[en] = b
}

func (net *Network) getClientEndInfo(en interface{}) (enabled bool,
	serverName interface{}, server *Server,
	reliable bool, longdelays bool, reordering bool) {
	net.Lock()
        defer net.Unlock()
	enabled = net.enabled[en]
	serverName = net.connections[en]
	server = net.servers[serverName]
	reliable = net.reliable
	longdelays = net.longDelays
	reordering = net.reordering
	return
}

func (net *Network) isServerDead(en interface{}, sn interface{}, svr *Server) bool {
	net.Lock()
	defer net.Unlock()
	if !net.enabled[en] || net.servers[sn] != svr {
		return true
	}
	return false
}

func (net *Network) ProcessRequest(req reqMsg) {
	enabled, sn, svr, reliable, ld, ro := net.getClientEndInfo(req.endName)
	if enabled && sn != nil && svr != nil {
		if !reliable {
			// regular transmission delay
			ms := rand.Int() % 27
			time.Sleep(time.Duration(ms) * time.Millisecond)
			// possible packet loss
			if (rand.Int() % 1000) < 100 {
				req.replyCh <- replyMsg{false, nil}
				return
			}
		}
		// rpc is handled here
		// create a channel to wait for reply
		repCh := make(chan replyMsg)
		go func() {
			r := svr.dispatch(req)
			repCh <- r
		}()
		var reply replyMsg
		repOk, svrDead := false, false
		for !repOk && !svrDead {
			select {
			case reply = <-repCh:
				repOk = true
			case <-time.After(100 * time.Millisecond):
				svrDead = net.isServerDead(req.endName, sn, svr)
			}
		}
		svrDead = net.isServerDead(req.endName, sn, svr)
		if !repOk || svrDead {
			req.replyCh <- replyMsg{false, nil}
		} else if !reliable && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil}
		} else if ro && rand.Intn(900) < 600 {
			ms := 200 + rand.Intn(1+rand.Intn(2000))
			time.Sleep(time.Duration(ms) * time.Millisecond)
			req.replyCh <- reply
		} else {
			req.replyCh <- reply
		}
	} else {
		// no reply and timeout
		ms := 0
		if ld {
			ms = rand.Int() % 7000
		} else {
			ms = rand.Int() % 100
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		req.replyCh <- replyMsg{false, nil}
	}
}
