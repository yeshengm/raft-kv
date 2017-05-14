package rpc

import (
	"log"
	"strings"
	"sync"
)

// services get registered to servers
type Server struct {
	sync.Mutex
	services map[string]*Service
	count    int
}

func MakeServer() *Server {
	s := &Server{
		services: make(map[string]*Service),
	}
	return s
}

func (s *Server) Register(svc *Service) {
	s.Lock()
	defer s.Unlock()
	s.services[svc.name] = svc
}

func (s *Server) GetCount() int {
	s.Lock()
	defer s.Unlock()
	return s.count
}

func (s *Server) dispatch(req reqMsg) replyMsg {
	s.Lock()
	s.count += 1
	dot := strings.LastIndex(req.svcMethod, ".")
	serviceName := req.svcMethod[:dot]
	methodName := req.svcMethod[dot+1:]
	service, ok := s.services[serviceName]
	s.Unlock()
	if ok {
		return service.dispatch(methodName, req)
	} else {
		log.Fatalf("server %v does not have remote method %v", s, req.svcMethod)
		return replyMsg{false, nil}
	}
}
