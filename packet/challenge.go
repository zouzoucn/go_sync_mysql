package packet

import (
	"github.com/goMySQLSemiSync/protocol"
	"github.com/goMySQLSemiSync/util"
	"github.com/wonderivan/logger"
)

/* Handshake packet
1              [0a] protocol version
string[NUL]    server version
4              connection id
string[8]      auth-plugin-data-part-1    challenge1
1              [00] filler
2              capability flags (lower 2 bytes)
if more data in the packet:
1              character set
2              status flags
2              capability flags (upper 2 bytes)
if capabilities & CLIENT_PLUGIN_AUTH {
1              length of auth-plugin-data
} else {
1              [00]
}
string[10]     reserved (all [00])
if capabilities & CLIENT_SECURE_CONNECTION {
string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))   challenge2
if capabilities & CLIENT_PLUGIN_AUTH {
string[NUL]    auth-plugin name
}
*/
type Challenge struct {
	*protocol.Packet
	protocolVersion int
	serverVersion string
	connectionId int
	challenge1 string
	capabilityFlags int
	characterSet int
	statusFlags int
	challenge2 string
	authPluginDataLength int
	authPluginName string
}

func NewChallenge() *Challenge{
	return &Challenge{
		Packet:               protocol.NewPacket(),
		protocolVersion:      0x0a,
		serverVersion:        "",
		connectionId:         0,
		challenge1:           "",
		capabilityFlags:      protocol.CLIENT_PROTOCOL_41,
		characterSet:         0,
		statusFlags:          0,
		challenge2:           "",
		authPluginDataLength: 0,
		authPluginName:       "",
	}
}

func (c *Challenge) GetChallenge1() string {
	return c.challenge1
}

func (c *Challenge) GetChallenge2() string {
	return c.challenge2
}

func (c *Challenge) setCapabilityFlag(flag int) {
	c.capabilityFlags |= flag
}

func (c *Challenge) GetCapabilityFlag() int{
	return c.capabilityFlags
}

func (c *Challenge) GetAuthPluginName() string{
	return c.authPluginName
}

//按位取反
func (c *Challenge) removeCapabilityFlag(flag int) {
	c.capabilityFlags &= ^flag
}

//按位异或
func (c *Challenge) toggleCapabilityFlag(flag int) {
	c.capabilityFlags ^= flag
}

func (c *Challenge) hasCapabilityFlag(flag int) bool{
	return ((c.capabilityFlags & flag) == flag)
}

func (c *Challenge) setStatusFlag(flag int) {
	c.statusFlags |= flag
}

func (c *Challenge) removeStatusFlag(flag int) {
	c.statusFlags &= ^flag
}

func (c *Challenge) toggleStatusFlag(flag int) {
	c.statusFlags ^= flag
}

func (c *Challenge) hasStatusFlag(flag int) bool{
	return ((c.statusFlags & flag) == flag)
}

/*
* 将一个 challenge 解出 payload []byte
*/
func (c *Challenge)  getPayload() []byte{
	payload := make([]byte, 0)
	payload = append(payload, protocol.Build_fixed_int(1, c.protocolVersion)...)
	payload = append(payload, protocol.Build_null_str(c.serverVersion)...)
	payload = append(payload, protocol.Build_fixed_int(4, c.connectionId)...)
	payload = append(payload, protocol.Build_fixed_str(8, c.challenge1)...)
	payload = append(payload, protocol.Build_filler(1, 0x00)...)
	payload = append(payload, protocol.Build_fixed_int(2, c.capabilityFlags >> 16)...)
	payload = append(payload, protocol.Build_fixed_int(1, c.characterSet)...)
	payload = append(payload, protocol.Build_fixed_int(2, c.statusFlags)...)
	payload = append(payload, protocol.Build_fixed_int(2, c.capabilityFlags & 0xffff)...)

	if c.hasCapabilityFlag(protocol.CLIENT_PLUGIN_AUTH) {
		payload = append(payload, protocol.Build_fixed_int(1, c.authPluginDataLength)...)
	} else {
		payload = append(protocol.Build_filler(1, 0x00))
	}
	payload = append(payload, protocol.Build_filler(10, 0x00)...)

	if c.hasCapabilityFlag(protocol.CLIENT_SECURE_CONNECTION) {
		payload = append(payload, protocol.Build_fixed_str(int(util.Max(13, int64(c.authPluginDataLength-8))), c.challenge2)...)
	}

	if c.hasCapabilityFlag(protocol.CLIENT_PLUGIN_AUTH) {
		payload = append(payload, protocol.Build_null_str(c.authPluginName)...)
	}

	return payload
}

/*
* 从1个packet中解析出 Challenge
*/
func LoadFromPacket(p *protocol.Packet) *Challenge{
	c := NewChallenge()
	proto := protocol.NewProto(p.ToPacket(), 3)
	c.Packet.SequenceId = proto.Get_fixed_int(1)
	c.protocolVersion = proto.Get_fixed_int(1)
	c.serverVersion = proto.Get_null_str()
	c.connectionId = proto.Get_fixed_int(4)
	c.challenge1 = proto.Get_fixed_str(8)
	proto.Get_filler(1)
	c.capabilityFlags = proto.Get_fixed_int(2) << 16
	if proto.Has_remaining_data() {
		c.characterSet = proto.Get_fixed_int(1)
		c.statusFlags = proto.Get_fixed_int(2)
		c.setCapabilityFlag(proto.Get_fixed_int(2))

		if c.hasCapabilityFlag(protocol.CLIENT_PLUGIN_AUTH) {
			c.authPluginDataLength = proto.Get_fixed_int(1)
		} else {
			proto.Get_filler(1)
		}

		proto.Get_filler(10)

		if (c.hasCapabilityFlag(protocol.CLIENT_SECURE_CONNECTION)) {
			c.challenge2 = proto.Get_fixed_str(int(util.Max(13, int64(c.authPluginDataLength-8))))
		}

		if (c.hasCapabilityFlag(protocol.CLIENT_PLUGIN_AUTH)) {
			c.authPluginName = proto.Get_null_str()
		}
	}
	logger.Debug("c==== seqId: ", c.SequenceId, "; protocolVersion: ", c.protocolVersion, "; serverVersion: ", c.serverVersion, "; connectionId: ", c.connectionId, "; challenge1: ", c.challenge1,
		"; capabilityFlags: ", c.capabilityFlags, "; characterSet: ", c.characterSet, "; statusFlags: ", c.statusFlags, "; c.capabilityFlag: ", c.capabilityFlags,
		"; authPluginDataLength: ", c.authPluginDataLength, "; challenge2: ", c.challenge2, "; authPluginName: ", c.authPluginName)
	return c
}