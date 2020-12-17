package packet

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/goMySQLSemiSync/protocol"
)

type GtidEvent struct {
	*protocol.Packet
	commit_flag bool
	sid string
	gno uint64
	gtid string
}

func (this *GtidEvent) GetGtid() string {
	return this.gtid
}

func NewGtidEvent() *GtidEvent {
	return &GtidEvent{
		Packet:      protocol.NewPacket(),
		commit_flag: false,
		sid:         "",
		gno:         0,
		gtid:        "",
	}
}

func (this *GtidEvent) LoadFromPacket(packet []byte) {
	proto := protocol.NewProto(packet, 0)
	this.commit_flag = (proto.Get_fixed_int(1) == 1)
	this.sid = string(proto.Read(16))
	this.gno = binary.LittleEndian.Uint64(proto.Read(8))

	hexSidData := hex.EncodeToString([]byte(this.sid))
	this.gtid = fmt.Sprintf("%s-%s-%s-%s-%s:%d", hexSidData[:8], hexSidData[8:12], hexSidData[12:16], hexSidData[16:20], hexSidData[20:], this.gno)
}

