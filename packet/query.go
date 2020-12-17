package packet

import "github.com/goMySQLSemiSync/protocol"

type Query struct {
	*protocol.Packet
	query string
}

func (q *Query) GetPacket() *protocol.Packet {
	return q.Packet
}

func (q *Query) SetPacket(packet *protocol.Packet) {
	q.Packet = packet
}

func (q *Query) GetQuery() string {
	return q.query
}

func (q *Query) SetQuery(query string) {
	q.query = query
}

func NewQuery() *Query{
	return &Query{
		Packet: protocol.NewPacket(),
		query:  "",
	}
}

func (q *Query) GetPayload() []byte{
	payload := make([]byte, 0)
	payload = append(payload, protocol.Build_byte(byte(protocol.COM_QUERY))...)
	payload = append(payload, protocol.Build_eof_str(q.query)...)
	return payload
}

func LoadFromPacketToQuery(packet *protocol.Packet) *Query{
	query := NewQuery()
	query.Packet = packet
	proto := protocol.NewProto(packet.ToPacket(), 3)
	query.SequenceId = proto.Get_fixed_int(1)
	proto.Get_filler(1)
	query.query = proto.Get_eof_str()

	return query
}