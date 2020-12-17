package dump

import (
	"encoding/binary"
	"fmt"
	"github.com/goMySQLSemiSync/constants"
	"github.com/goMySQLSemiSync/packet"
	"github.com/goMySQLSemiSync/protocol"
	"github.com/wonderivan/logger"
	"os"
)

type BinlogReaderStream struct {
	*BaseStream
	currentLogFile string // 启动后开始dump的binlog文件名
	currentLogPos  int64  // 启动后开始dump的binlog pos地址
	auto_position  bool
	has_register_slave bool
	binlog_header_fix_length int
}

func (brs *BinlogReaderStream) SetBasestream(basestream *BaseStream) {
	brs.BaseStream = basestream
}

func (brs *BinlogReaderStream) GetBasestream() *BaseStream{
	return brs.BaseStream
}

func (brs *BinlogReaderStream) SetCurrentLogFile(currentLogFile string) {
	brs.currentLogFile = currentLogFile
}

func (brs *BinlogReaderStream) GetCurrentLogFile() string{
	return brs.currentLogFile
}

func (brs *BinlogReaderStream) SetCurrentLogPos(currentLogPos int64) {
	brs.currentLogPos = currentLogPos
}

func (brs *BinlogReaderStream) GetCurrentLogPos() int64{
	return brs.currentLogPos
}

func (brs *BinlogReaderStream) SetAuto_position(auto_position bool) {
	brs.auto_position = auto_position
}

func (brs *BinlogReaderStream) GetAuto_position() bool{
	return brs.auto_position
}

func (brs *BinlogReaderStream) SetHas_register_slave(has_register_slave bool) {
	brs.has_register_slave = has_register_slave
}

func (brs *BinlogReaderStream) GetHas_register_slave() bool{
	return brs.has_register_slave
}

func (brs *BinlogReaderStream) SetBinlog_header_fix_length(binlog_header_fix_length int) {
	brs.binlog_header_fix_length = binlog_header_fix_length
}

func (brs *BinlogReaderStream) GetBinlog_header_fix_length() int{
	return brs.binlog_header_fix_length
}

func newBinlogReaderStream(basestream *BaseStream, currentLogFile string, currentLogPos int64, auto_position bool) *BinlogReaderStream{
	brs :=  &BinlogReaderStream{
		BaseStream:   basestream,
		currentLogFile: currentLogFile,
		currentLogPos:  currentLogPos,
		auto_position: auto_position,
		has_register_slave: false,
	}

	if brs.binlogServer.semiSync == true {
		brs.binlog_header_fix_length = 7
	} else {
		brs.binlog_header_fix_length = 5
	}
	return brs
}

func (brs *BinlogReaderStream) Register_slave() {
	if brs.has_register_slave {
		return
	}

	masterId := brs.binlogServer.masterId
	port := brs.binlogServer.port
	serverId := brs.binlogServer.serverId
	serverUuid := brs.binlogServer.serverUuid
	heartbeatPeriod := brs.binlogServer.heartbeatPeriod

	sql := packet.NewQuery()
	sql.SequenceId = 0
	sql.SetQuery(fmt.Sprintf("SET @slave_uuid= '%s'", serverUuid))
	sql.Payload = sql.GetPayload()
	packetToByte := sql.ToPacket()
	brs.send_packet(packetToByte)
	brs.read_packet()

	sql = packet.NewQuery()
	sql.SequenceId = 0
	sql.SetQuery(fmt.Sprintf("SET @master_heartbeat_period= %d", heartbeatPeriod))
	sql.Payload = sql.GetPayload()
	packetToByte = sql.ToPacket()
	brs.send_packet(packetToByte)
	brs.read_packet()

	slave := packet.NewSlave()
	slave.SetPort(port)
	slave.SetMasterId(masterId)
	slave.SetServerId(serverId)
	slave.SequenceId = 0
	payload := slave.GetPayload()
	brs.send_packet(payload)
	brs.read_packet()

	//是否启用半同步
	if brs.binlogServer.semiSync {
		sql = packet.NewQuery()
		sql.SequenceId = 0
		sql.SetQuery("SET @rpl_semi_sync_slave = 1")
		sql.Payload = sql.GetPayload()
		packetToByte = sql.ToPacket()
		brs.send_packet(packetToByte)
		brs.read_packet()
	}

	if brs.auto_position {

		dump := packet.NewDumpGtid()
		gtid_purged := brs.binlogServer.gtid_purged
		gtid_set := dump.GetPurgedGtidSet(gtid_purged)

		dump.SetGtidSet(gtid_set)
		dump.SetServerId(serverId)
		dump.SetAuto_position(brs.auto_position)
		dump.SequenceId = 0
		packet := dump.GetPayload()
		brs.send_packet(packet)
		brs.read_packet()
		brs.has_register_slave = true
	} else {
		dump := packet.NewDumpPos()
		dump.SetServerId(serverId)
		dump.SetLogFile(brs.currentLogFile)
		dump.SetLogPos(brs.currentLogPos)
		dump.SequenceId = 0
		packet := dump.GetPayload()
		brs.send_packet(packet)
		brs.read_packet()
		brs.has_register_slave = true
	}
}

func (this *BinlogReaderStream) Fetchone() (uint32, int, uint32, uint32, []byte){
	for {
		this.Register_slave()
		packetread := this.read_packet()
		//sequenceId := packetread.GetSequenceId()
		packetType := packetread.GetType()
		packetSlice := packetread.ToPacket()
		pos := this.binlog_header_fix_length

		// header
		timestamp := binary.LittleEndian.Uint32(packetSlice[pos:pos + 4])
		pos += 4
		event_type := int(packetSlice[pos:pos + 1][0])
		pos += 1
		//server_id := binary.LittleEndian.Uint32(packetSlice[pos:pos + 4])
		_ = binary.LittleEndian.Uint32(packetSlice[pos:pos + 4])
		pos += 4
		event_size := binary.LittleEndian.Uint32(packetSlice[pos:pos + 4])
		pos += 4
		log_pos := binary.LittleEndian.Uint32(packetSlice[pos:pos + 4])
		pos += 4
		//flags := binary.LittleEndian.Uint16(packetSlice[pos:pos+2])
		_ = binary.LittleEndian.Uint16(packetSlice[pos:pos+2])

		//跳过HEARTBEAT_EVENT
		if event_type == constants.HEARTBEAT_EVENT {
			continue
		}
		//跳过重启后的第一个FORMAT_DESCRIPTION_EVENT
		if event_type == constants.FORMAT_DESCRIPTION_EVENT && log_pos == 0 {
			continue
		}

		//如果是半同步复制
		if this.binlogServer.semiSync {
			if event_type == constants.XID_EVENT || event_type == constants.QUERY_EVENT {
				ack := packet.NewSemiAck()
				ack.SetLogPos(uint32(this.currentLogPos))
				ack.SetLogFile(this.currentLogFile)
				ack.Packet.SequenceId = 0
				ackPacket := ack.ToPacket()
				this.send_packet(ackPacket)
			}
		}

		if packetType == byte(protocol.ERR) {
			err := protocol.LoadFromPacket(packetread)
			logger.Error("error: errorcode %d, sqlstate %s, errorMessage %s", err.GetErrCode(), err.GetSqlState(), err.GetErrorMessage())
			this.Close()
			os.Exit(1)
		}
		return timestamp, event_type, event_size, log_pos, packetSlice[this.binlog_header_fix_length:]
	}
}