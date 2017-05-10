package rpc

type ClientEnd struct {
	endName interface{}
	ch      chan reqMsg
}
