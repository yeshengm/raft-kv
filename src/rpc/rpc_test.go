package rpc

import (
	"fmt"
	"testing"
)

func TestImport(t *testing.T) {
	net := MakeNetwork()
	fmt.Println(net.reliable)
}
