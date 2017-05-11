package rpc

import (
	"fmt"
	"testing"
)

func TestNetconf(t *testing.T) {
	net := MakeNetwork()
	// reliable
	if net.reliable == false {
		t.Errorf("network should be set to reliable")
	}
	// set unreliable
	net.Reliable(false)
	if net.reliable == true {
		t.Errorf("network has already been set to unreliable")
	}
	// add client
	for i := 0; i < 5; i++ {
		net.AddClientEnd(fmt.Sprintf("client:%v", i))
	}
	if _, ok := net.ends["client:4"]; !ok {
		t.Errorf("client 4 is not added to net.ends")
	}
	if b, _ := net.enabled["client:3"]; b {
		t.Errorf("client end should by default be disabled")
	}
	// add server
	for i := 0; i < 3; i++ {
		net.AddServer(fmt.Sprintf("s:%v", i), &Server{})
	}
	if _, ok := net.servers["s:0"]; !ok {
		t.Error("server 0 is not added to net.servers")
	}
	// delete server
	net.DeleteServer("s:1")
	if s, _ := net.servers["s:1"]; s != nil {
		t.Error("server 1 is not properly deleted")
	}
	// connect end to server
	net.Connect("client:3", "s:0")
	if net.connections["client:3"] != "s:0" {
		t.Error("client3 and server0 is not properly connected")
	}
	// enable a client end
	net.Enable("client:1", true)
	if !net.enabled["client:1"] {
		t.Error("client1 is not enabled")
	}
}
