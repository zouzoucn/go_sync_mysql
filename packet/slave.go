package packet

import (
	"bytes"
	"encoding/binary"
	"github.com/goMySQLSemiSync/protocol"
	"github.com/goMySQLSemiSync/util"
)

type Slave struct {
	*protocol.Packet
	hostname string
	username string
	password string
	port int
	masterId int
	serverId int
}

func (s *Slave) SetPacket(packet *protocol.Packet) {
	s.Packet = packet
}

func (s *Slave) GetPacket() *protocol.Packet {
	return s.Packet
}

func (s *Slave) SetHostname(hostname string) {
	s.hostname = hostname
}

func (s *Slave) GetHostname() string {
	return s.hostname
}

func (s *Slave) SetUsername(username string) {
	s.username = username
}

func (s *Slave) GetUsername() string {
	return s.username
}

func (s *Slave) SetPassword(password string) {
	s.password = password
}

func (s *Slave) GetPassword() string {
	return s.password
}

func (s *Slave) SetPort(port int) {
	s.port = port
}

func (s *Slave) GetPort() int {
	return s.port
}

func (s *Slave) SetServerId(serverId int) {
	s.serverId = serverId
}

func (s *Slave) GetServerId() int {
	return s.serverId
}

func (s *Slave) SetMasterId(masterId int) {
	s.masterId = masterId
}

func (s *Slave) GetMasterId() int {
	return s.masterId
}

func NewSlave() *Slave {
	return &Slave{
		Packet:   protocol.NewPacket(),
		hostname: "",
		username: "",
		password: "",
		port:     0,
		masterId: 0,
		serverId: 0,
	}
}

/*
        # 0              payload.length
*       # 1              [15] COM_REGISTER_SLAVE
        # 4              server-id
        # 1              slaves hostname length
        # string[$len]   slaves hostname
        # 1              slaves user len
        # string[$len]   slaves user
        # 1              slaves password len
        # string[$len]   slaves password
        # 2              slaves mysql-port
        # 4              replication rank
        # 4              master-id
*/
func (s *Slave) GetPayload() []byte{
	payload := make([]byte, 0)
	lhostname := len(s.hostname)
	lusername := len(s.username)
	lpassword := len(s.password)

	packetLen := (1 + //command
	              4 + // server-id
	              1 + //hostname length
	              lhostname + //hostname
	              1 + //username length
	              lusername +
		          1 + //password length
	              lpassword + //password
	              2 + // slave mysql port
	              4 + // replication rank
	              4) // master-id
	//MAX_STRING_LEN := 257
	var b_buf bytes.Buffer
	binary.Write(&b_buf, binary.LittleEndian, uint32(packetLen))
	util.FillBuffer(&b_buf, 4)

	payload = append(payload, b_buf.Bytes()...)

	payload = append(payload, byte(protocol.COM_REGISTER_SLAVE))

	b_buf.Reset()
	binary.Write(&b_buf, binary.LittleEndian, uint32(s.serverId))
	util.FillBuffer(&b_buf, 4)
	payload = append(payload, b_buf.Bytes()...)

	payload = append(payload, []byte(s.hostname)...)
	payload = append(payload, 0x00)

	payload = append(payload, []byte(s.username)...)
	payload = append(payload, 0x00)

	payload = append(payload, []byte(s.password)...)
	payload = append(payload, 0x00)

	b_buf.Reset()
	binary.Write(&b_buf, binary.LittleEndian, uint16(s.port))
	util.FillBuffer(&b_buf, 2)
	payload = append(payload, b_buf.Bytes()...)

	payload = append(payload, []byte{0x00, 0x00, 0x00, 0x00}...)

	b_buf.Reset()
	binary.Write(&b_buf, binary.LittleEndian, uint32(s.masterId))
	util.FillBuffer(&b_buf, 4)
	payload = append(payload, b_buf.Bytes()...)

	return payload
}

func LoadFromPacketToSlave(packet *protocol.Packet) *Slave{
	return nil
}



