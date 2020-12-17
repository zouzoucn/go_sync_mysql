package packet

import (
	"bytes"
	"github.com/goMySQLSemiSync/protocol"
)

type SemiAck struct {
	*protocol.Packet
	logFile string
	logPos uint32
}

func NewSemiAck() *SemiAck {
	return &SemiAck{
		Packet:  protocol.NewPacket(),
		logFile: "",
		logPos:  0,
	}
}

func (this *SemiAck) SetLogFile(logFile string) {
	this.logFile = logFile
}

func (this *SemiAck) SetLogPos(logPos uint32) {
	this.logPos = logPos
}

/**
  # 1 0xef kPacketMagicNum
  # 8 log_pos
  # n binlog_filename
*/
func (this *SemiAck) GetPayload() []byte {
	var buf bytes.Buffer
	buf.Write(protocol.Build_byte(0xef))
	buf.Write(protocol.Build_fixed_int(8, int(this.logPos)))
	buf.Write(protocol.Build_eof_str(this.logFile))

	return buf.Bytes()
}
