package packet

import (
	"bytes"
	"encoding/binary"
	"github.com/goMySQLSemiSync/constants"
	"github.com/goMySQLSemiSync/protocol"
)

type DumpPos struct {
	*protocol.Packet
	serverId int
	logFile string
	logPos int64
}

func NewDumpPos() *DumpPos {
	return &DumpPos{
		Packet:   protocol.NewPacket(),
		serverId: 0,
		logFile:  "",
		logPos:   0,
	}
}

func (this *DumpPos) SetServerId(serverId int) {
	this.serverId = serverId
}

func (this *DumpPos) SetLogFile(logFile string) {
	this.logFile = logFile
}

func (this *DumpPos) SetLogPos(logPos int64) {
	this.logPos = logPos
}

//func NewDumpPos(serverId int, logFile string, logPos int64) *DumpPos {
//	return &DumpPos{
//		Packet:   protocol.NewPacket(),
//		serverId: serverId,
//		logFile:  logFile,
//		logPos:   logPos,
//	}
//}

/**
1              [12] COM_BINLOG_DUMP
4              binlog-pos
2              flags
4              server-id
string[EOF]    binlog-filename
*/
func (this *DumpPos) GetPayload() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(len(this.logFile) + 11))
	buf.WriteByte(byte(constants.COM_BINLOG_DUMP))

	binary.Write(&buf, binary.LittleEndian, uint32(this.logPos))
	flags := 0
	binary.Write(&buf, binary.LittleEndian, uint16(flags))
	binary.Write(&buf, binary.LittleEndian, uint32(this.serverId))
	buf.WriteString(this.logFile)

	return buf.Bytes()
}