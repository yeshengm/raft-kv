package rpc

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"reflect"
)

// a service is set of methods related to a receiver
type Service struct {
	name    string
	rcvr    reflect.Value
	tpe     reflect.Type
	methods map[string]reflect.Method
}

// service constructor which heavily uses reflection
// an interface value consits of: Value and Type
func MakeService(rcvr interface{}) *Service {
	svc := &Service{}
	svc.tpe = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	svc.methods = map[string]reflect.Method{}

	// register those valid methods
	for m := 0; m < svc.tpe.NumMethod(); m++ {
		method := svc.tpe.Method(m)
		mtpe := method.Type
		mname := method.Name
		if method.PkgPath != "" ||
			mtpe.NumIn() != 3 ||
			mtpe.In(2).Kind() != reflect.Ptr ||
			mtpe.NumOut() != 0 {
			fmt.Printf("bad method %v\n", mname)
		} else {
			svc.methods[mname] = method
		}
	}
	return svc
}

func (svc *Service) dispatch(methname string, req reqMsg) replyMsg {
	if method, ok := svc.methods[methname]; ok {
		// create a cotainer for the deserialized args
		args := reflect.New(req.argsType)
		argBuf := bytes.NewBuffer(req.args)
		argDec := gob.NewDecoder(argBuf)
		argDec.Decode(args.Interface())

		replyType := method.Type.In(2)
		replyType = replyType.Elem()
		replyv := reflect.New(replyType)

		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyv})

		repBuf := new(bytes.Buffer)
		repEnc := gob.NewEncoder(repBuf)
		repEnc.Encode(replyv)
		return replyMsg{true, repBuf.Bytes()}
	} else {
		log.Fatalf("unknown method %v to call in service %v\n",
			methname, svc.name)
		return replyMsg{false, nil}
	}
}
