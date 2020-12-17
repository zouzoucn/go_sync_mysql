package packet

import (
	"github.com/goMySQLSemiSync/protocol"
)

type Response struct {
	*protocol.Packet
	capabilityFlags int
	maxPacketSize int
	characterSet int
	username string
	authResponse []byte
	schema string
	pluginName string
	clientAttributes map[string]string
}

func NewResponse() *Response{
	return &Response{
		Packet:           protocol.NewPacket(),
		capabilityFlags:  protocol.CLIENT_PROTOCOL_41,
		maxPacketSize:    0,
		characterSet:     0,
		username:         "",
		authResponse:     []byte{},
		schema:           "",
		pluginName:       "",
		clientAttributes: make(map[string]string),
	}
}

func (r *Response) SetCapablityFlag(flag int) {
	r.capabilityFlags = flag
}

func (r *Response) GetCapablityFlag() int{
	return r.capabilityFlags
}

func (r *Response) SetMaxPacketSize(maxPacketSize int) {
	r.maxPacketSize = maxPacketSize
}

func (r *Response) GetMaxPacketSize() int{
	return r.maxPacketSize
}

func (r *Response) SetCharacterSet(characterSet int) {
	r.characterSet = characterSet
}

func (r *Response) GetCharacterSet() int{
	return r.characterSet
}

func (r *Response) SetUsername(username string) {
	r.username = username
}

func (r *Response) GetUsername() string{
	return r.username
}

func (r *Response) SetAuthResponse(authResponse []byte) {
	r.authResponse = authResponse
}

func (r *Response) GetAuthResponse() []byte{
	return r.authResponse
}

func (r *Response) SetSchema(schema string) {
	r.schema = schema
}

func (r *Response) GetSchema() string{
	return r.schema
}

func (r *Response) SetPluginName(pluginName string) {
	r.pluginName = pluginName
}

func (r *Response) GetPluginName() string{
	return r.pluginName
}

func (r *Response) SetClientAttributes(clientAttributes map[string]string) {
	r.clientAttributes = clientAttributes
}

func (r *Response) GetClientAttributes() map[string]string{
	return r.clientAttributes
}


func (r *Response) AddCapablityFlag(flag int) {
	r.capabilityFlags |= flag
}

func (r *Response) RemoveCapablityFlag(flag int) {
	r.capabilityFlags &= ^flag
}

func (r *Response) ToggleCapablityFlag(flag int) {
	r.capabilityFlags ^= flag
}

func (r *Response) HasCapablityFlag(flag int) bool {
	return (r.capabilityFlags & flag) == flag
}

func (r *Response) GetPayload() []byte{
	payload := make([]byte, 0)
	if r.HasCapablityFlag(protocol.CLIENT_PROTOCOL_41) {
		payload = append(payload, protocol.Build_fixed_int(4, r.capabilityFlags)...)
		payload = append(payload, protocol.Build_fixed_int(4, r.maxPacketSize)...)
		payload = append(payload, protocol.Build_fixed_int(1, r.characterSet)...)
		payload = append(payload, protocol.Build_filler(23, 0x00)...)
		payload = append(payload, protocol.Build_null_str(r.username)...)

		if r.HasCapablityFlag(protocol.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
			payload = append(payload, protocol.Build_lenenc_int(len(r.authResponse))...)
			payload = append(payload, protocol.Build_fixed_str(len(r.authResponse), string(r.authResponse))...)
		} else if r.HasCapablityFlag(protocol.CLIENT_SECURE_CONNECTION) {
			payload = append(payload, protocol.Build_lenenc_int(len(r.authResponse))...)
			payload = append(payload, protocol.Build_fixed_str(len(r.authResponse), string(r.authResponse))...)
		} else {
			payload = append(payload, protocol.Build_null_str(string(r.authResponse))...)
		}

		if r.HasCapablityFlag(protocol.CLIENT_CONNECT_WITH_DB) {
			payload = append(payload, protocol.Build_null_str(r.schema)...)
		}

		if r.HasCapablityFlag(protocol.CLIENT_PLUGIN_AUTH) {
			payload = append(payload, protocol.Build_null_str(r.pluginName)...)
		}

		if r.HasCapablityFlag(protocol.CLIENT_CONNECT_ATTRS) {
			attributes := make([]byte, 0)
			for key, value := range r.clientAttributes {
				attributes = append(attributes, protocol.Build_lenenc_str(key)...)
				attributes = append(attributes, protocol.Build_lenenc_str(value)...)
			}
			payload = append(payload, protocol.Build_lenenc_int(len(attributes))...)
			payload = append(payload, attributes...)
		}
	} else {
		payload = append(payload, protocol.Build_fixed_int(2, r.capabilityFlags)...)
		payload = append(payload, protocol.Build_fixed_int(3, r.maxPacketSize)...)
		payload = append(payload, protocol.Build_null_str(r.username)...)
		if r.HasCapablityFlag(protocol.CLIENT_CONNECT_WITH_DB) {
			payload = append(payload, protocol.Build_null_str(string(r.authResponse))...)
			payload = append(payload, protocol.Build_null_str(r.schema)...)
		} else {
			payload = append(payload, protocol.Build_eof_str(string(r.authResponse))...)
		}
	}
	return payload
}

func LoadFromPacketToResponse(packet *protocol.Packet) *Response{
	r := NewResponse()
	proto := protocol.NewProto(packet.ToPacket(), 3)
	r.SequenceId = proto.Get_fixed_int(1)
	r.capabilityFlags = proto.Get_fixed_int(2)
	proto.SetOffset(proto.GetOffset() - 2)

	if r.HasCapablityFlag(protocol.CLIENT_PROTOCOL_41) {
		r.capabilityFlags = proto.Get_fixed_int(4)
		r.maxPacketSize = proto.Get_fixed_int(4)
		r.characterSet = proto.Get_fixed_int(1)
		proto.Get_filler(23)
		r.username = proto.Get_null_str()

		if r.HasCapablityFlag(protocol.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
			authResponseLen := proto.Get_lenenc_int()
			r.authResponse = []byte(proto.Get_fixed_str(authResponseLen))
		} else if r.HasCapablityFlag(protocol.CLIENT_SECURE_CONNECTION) {
			authResponseLen := proto.Get_lenenc_int()
			r.authResponse = []byte(proto.Get_fixed_str(authResponseLen))
		} else {
			r.authResponse = []byte(proto.Get_null_str())
		}

		if r.HasCapablityFlag(protocol.CLIENT_CONNECT_WITH_DB) {
			r.schema = proto.Get_null_str()
		}

		if r.HasCapablityFlag(protocol.CLIENT_PLUGIN_AUTH) {
			r.pluginName = proto.Get_null_str()
		}

		if r.HasCapablityFlag(protocol.CLIENT_CONNECT_ATTRS) {
			proto.Get_lenenc_int()
			for {
					if proto.Has_remaining_data() {
						key := proto.Get_lenenc_str()
						value := proto.Get_lenenc_str()
						r.clientAttributes[key] = value
					} else {
						break
					}
			}
		}
	} else {
		r.capabilityFlags = proto.Get_fixed_int(2)
		r.maxPacketSize = proto.Get_fixed_int(3)
		r.username = proto.Get_null_str()
		if r.HasCapablityFlag(protocol.CLIENT_CONNECT_WITH_DB) {
			r.authResponse = []byte(proto.Get_null_str())
			r.schema = proto.Get_null_str()
		} else {
			r.authResponse = []byte(proto.Get_eof_str())
		}
	}
	return r
}


