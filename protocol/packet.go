package protocol

import (
	"encoding/binary"
)

// MySQL包
//Type	Name	Description
//int<3>	payload_length	Length of the payload. The number of bytes in the packet beyond the initial 4 bytes that make up the packet header.
//int<1>	sequence_id	Sequence ID
//string<var>	payload	[len=payload_length] payload of the packet
type Packet struct {
	Length int
	SequenceId int
	Payload []byte
}

func NewPacket() *Packet{
	return &Packet{
		Length:     0,
		SequenceId: 0,
		Payload:    nil,
	}
}

func (p *Packet) GetPayload() []byte{
	return p.Payload
}

//将一个packet对象转换为[]byte
func (p *Packet) ToPacket() []byte{
	payload := p.Payload
	size := len(payload)

	packet := make([]byte, 0, size + 4)
	packet = append(packet, Build_fixed_int(3, size)...)
	packet = append(packet, Build_fixed_int(1, p.SequenceId)[0])
	packet = append(packet, payload...)
	return packet
}

//将一个 []byte 转换为 Packet
func ToPacket(bs []byte) (*Packet, error) {
	lenSlice := bs[0:3]
	len := binary.LittleEndian.Uint32(append(lenSlice, 0x00))
	//var len uint32
	//b_buf := bytes.NewBuffer(lenSlice)
	//err := binary.Read(b_buf, binary.LittleEndian, &len)
	//if err != nil {
	//	return nil, errors.New(err.Error())
	//}

	seqByte := bs[3:4][0]
	seqId := int(seqByte)

	//seqSlice := bs[3:4]
	//var seqId int32
	//b_buf = bytes.NewBuffer(seqSlice)
	//err = binary.Read(b_buf, binary.LittleEndian, &seqId)
	//if err != nil {
	//	return nil, errors.New(err.Error())
	//}

	payload := bs[4:]
	packet := &Packet{
		Length:     int(len),
		SequenceId: int(seqId),
		Payload:    payload,
	}
	return packet, nil
}

func GetSize(packet []byte) int{
	return NewProto(packet, 0).Get_fixed_int(3)
}

func (p *Packet) GetType() byte {
	return p.Payload[0]
}

func (p *Packet) GetSequenceId() int{
	return p.SequenceId
}

func (p *Packet) dump() {

}

//func (p *Packet) read_server_packet(socket_in) {
//
//}

func (p *Packet) file2packet(filename string) {

}

func (p *Packet) dump_my_packet() {

}