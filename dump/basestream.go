package dump

import (
	"encoding/binary"
	"fmt"
	"github.com/goMySQLSemiSync/packet"
	"github.com/goMySQLSemiSync/protocol"
	"github.com/wonderivan/logger"
	"net"
	"os"
	"time"
)

type BaseStream struct {
	binlogServer *BinlogServer
	conn         *net.Conn
}

func NewBaseStream(b *BinlogServer) *BaseStream{
	bs := &BaseStream{
		binlogServer: b,
		conn:         nil,
	}
	bs.getConn()
	return bs
}

func (b *BaseStream) send_packet(buff []byte) {
	skt := *(b.conn)
	skt.Write(buff)
}

func (b *BaseStream) read_packet() *protocol.Packet{
	socketIn := *(b.conn)
	packet_length := make([]byte, 4)
	socketIn.Read(packet_length)  // payload_length + seq_id

	psize := uint(binary.LittleEndian.Uint16(packet_length[:2]))
	bytes_to_read := psize + uint(packet_length[2:3][0])<<16

	packet_payload := make([]byte, bytes_to_read)
	socketIn.Read(packet_payload)  // payload

	packetSlice := append(packet_length, packet_payload...)
	packet, err := protocol.ToPacket(packetSlice)
	if err != nil {
		logger.Error(err.Error())
	}
	packet.Payload = packet.GetPayload()
	packetType := packet.GetType()
	if packetType == byte(protocol.ERR) {
		err := protocol.LoadFromPacket(packet)
		logger.Error("error, read packet from mysql error, errorCode: ", err.GetErrCode(), " sqlState: ", err.GetSqlState(), " errorMessage: ", err.GetErrorMessage())
		socketIn.Close()
		os.Exit(1)
	}
	return packet
}

func (b *BaseStream) getConn() {
	addr := fmt.Sprintf("%s:%d", b.binlogServer.host, b.binlogServer.port)
	user := b.binlogServer.user
	schema := ""
	conn, err := net.DialTimeout("tcp", addr, 1000 * time.Second)
	if err != nil {
		logger.Error("conn to mysql error, err: ", err.Error())
	}
	b.conn = &conn

	challenge := packet.LoadFromPacket(b.read_packet())
	challenge1 := challenge.GetChallenge1()
	challenge2 := challenge.GetChallenge2()

	scramble_password := protocol.Scramble_native_password([]byte(b.binlogServer.password), []byte(fmt.Sprintf("%s%s", challenge1, challenge2)))
	response := packet.NewResponse()
	response.SequenceId = 1
	response.SetCapablityFlag(33531397)
	response.SetCharacterSet(33)
	response.SetMaxPacketSize(16777216)
	clientAttributes := make(map[string]string)
	clientAttributes["_client_name"] = "gomysql"
	clientAttributes["_pid"] = string(os.Getpid())
	clientAttributes["_client_version"] = "5.7"
	clientAttributes["program_name"] = "mysql"
	response.SetClientAttributes(clientAttributes)
	response.SetPluginName(challenge.GetAuthPluginName())
	//response.SetPluginName("mysql_native_password")
	response.SetUsername(user)
	response.SetSchema(schema)
	response.SetAuthResponse(scramble_password)

	response.RemoveCapablityFlag(protocol.CLIENT_COMPRESS)
	response.RemoveCapablityFlag(protocol.CLIENT_SSL)
	response.RemoveCapablityFlag(protocol.CLIENT_LOCAL_FILES)
	response.Payload = response.GetPayload()
	b.send_packet(response.ToPacket())
	//time.Sleep(time.Second * 100)
	packet := b.read_packet()
	packetType := packet.GetType()
	if packetType == byte(protocol.ERR) {
		err := protocol.LoadFromPacket(packet)
		logger.Error("error: ", "errorCode: ", err.GetErrCode(), " sqlState: ", err.GetSqlState(), " errorMessage: ", err.GetErrorMessage())
		b.Close()
		os.Exit(1)
	}
}

func (b *BaseStream) Close() {
	(*(b.conn)).Close()
}