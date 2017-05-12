package rpc

import (
	"bytes"
	"encoding/gob"
	"log"
	"reflect"
)

type ClientEnd struct {
	endName interface{}
	reqCh   chan reqMsg // channel acts like a reference type
}

// core of a rpc call, adapted from stdlib net/rpc
// a callable rpc method should statisty following properties:
// 1. type and method should be exported
// 2. the second argument contains the arguments to be passed in
// 3. the third argument should be a pointer to store reply result
func (client *ClientEnd) Call(svcMthd string, args interface{}, reply interface{}) bool {
	// encode args and send rpc request
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(args)
	req := reqMsg{
		endName:   client.endName,
		svcMethod: svcMthd,
		argsType:  reflect.TypeOf(args),
		args:      buf.Bytes(),
		replyCh:   make(chan replyMsg),
	}

	// send to reqCh and wait for result
	client.reqCh <- req
	rep := <-req.replyCh
	// deserialize
	if rep.ok {
		repBuf := bytes.NewBuffer(rep.reply)
		dec := gob.NewDecoder(repBuf)
		if err := dec.Decode(reply); err != nil {
			log.Fatalf("RPC call decode error\n")
		}
		return true
	}
	return false
}
