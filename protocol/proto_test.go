package protocol

import (
	"fmt"
	"testing"
)

func TestBuild_fixed_int(t *testing.T) {
	p := newProto(make([]byte, 0), 0)
	packet := p.build_fixed_int(2, 0xFFFF)
	fmt.Println(packet)
}

