package rpc

import "reflect"

type reqMsg struct {
	endName   interface{}
	svcMethod string
	argsType  reflect.Type // type info to reconstruct args
	args      []byte
	replyCh   chan replyMsg // channel to send back result
}

type replyMsg struct {
	ok    bool
	reply []byte
}
