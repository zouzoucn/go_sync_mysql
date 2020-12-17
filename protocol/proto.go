package protocol

import (
	"crypto/sha1"
	"github.com/wonderivan/logger"
	"math"
)

type Proto struct {
	packet []byte       // type packet.toPacket() -> []byte
	offset int
}

func NewProto(packet []byte, offset int) *Proto{
	return &Proto {
		packet: packet,
		offset: offset,
	}
}


func (p *Proto) SetPacket(packet []byte) {
	p.packet = packet
}

func (p *Proto) SetOffset(offset int) {
	p.offset = offset
}

func (p *Proto) GetPacket() []byte{
	return p.packet
}

func (p *Proto) GetOffset() int{
	return p.offset
}

func (p *Proto) Has_remaining_data() bool{
	return len(p.packet) - p.offset > 0
}

func Build_fixed_int(size int, value int) []byte{
	packet := make([]byte, size)
	if size >= 1 {
		packet[0] = byte((value >> 0) & 0xFF)
	}
	if size >= 2 {
		packet[1] = byte((value >> 8) & 0xFF)
	}
	if size >= 3 {
		packet[2] = byte((value >> 16) & 0xFF)
	}
	if size >= 4 {
		packet[3] = byte((value >> 24) & 0xFF)
	}
	if size >= 8 {
		packet[4] = byte((value >> 32) & 0xFF)
		packet[5] = byte((value >> 40) & 0xFF)
		packet[6] = byte((value >> 48) & 0xFF)
		packet[7] = byte((value >> 56) & 0xFF)
	}
	return packet
}

func Build_lenenc_int(value int) []byte{
	var packet []byte
	if (value < 251) {
		packet = make([]byte, 1)
		packet[0] = byte((value >> 0) & 0xFF)
	} else if (float64(value) < (math.Pow(2, 16) - 1)) {
		packet = make([]byte, 3)
		packet[0] = 0xFC
		packet[1] = byte((value >> 0) & 0xFF)
		packet[2] = byte((value >> 8) & 0xFF)
	} else if (float64(value) < (math.Pow(2, 24) - 1)) {
		packet = make([]byte, 4)
		packet[0] = 0xFD
		packet[1] = byte((value >> 0) & 0xFF)
		packet[2] = byte((value >> 8) & 0xFF)
		packet[3] = byte((value >> 16) & 0xFF)
	} else {
		packet = make([]byte, 9)
		packet[0] = 0xFE
		packet[1] = byte((value >> 0) & 0xFF)
		packet[2] = byte((value >> 8) & 0xFF)
		packet[3] = byte((value >> 16) & 0xFF)
		packet[4] = byte((value >> 24) & 0xFF)
		packet[5] = byte((value >> 32) & 0xFF)
		packet[6] = byte((value >> 40) & 0xFF)
		packet[7] = byte((value >> 48) & 0xFF)
		packet[8] = byte((value >> 56) & 0xFF)
	}
	return packet
}

func Build_fixed_str(size int, value string) []byte{
	packet := make([]byte, size)
	value2 := []byte(value)
	for i, c := range value2 {
		packet[i] = c
	}
	return packet
}

func Build_lenenc_str(value string) []byte{
	if value == "" {
		return make([]byte, 1)
	}
	size := Build_lenenc_int(len(value))
	fixed_str := Build_fixed_str(len(value), value)
	return append(size, fixed_str...)
}

func Build_null_str(value string) []byte{
	packet := Build_fixed_str(len(value) + 1, value)
	return packet
}

func Build_eof_str(value string) []byte {
	return Build_fixed_str(len(value), value)
}

func Build_filler(size int, fill byte) []byte{
	packet := make([]byte, size)
	for i := 0; i < size; i++ {
		packet[i] = fill
	}
	return packet
}

func Build_byte(value byte) []byte {
	packet := make([]byte, 1)
	packet[0] = value
	return packet
}

func Get_fixed_int_sniplet(packet []byte) int{
	value := 0x00
	for i := len(packet) - 1; i > 0; i-- {
		value |= int(packet[i]) & 0xFF
		value <<= 8
	}
	value |= (int(packet[0]) & 0xFF)
	return value
}

func (p *Proto) Get_fixed_int(size int) int{
	value := Get_fixed_int_sniplet(p.packet[p.offset:p.offset + size])
	p.offset += size
	return value
}

func (p *Proto) Get_filler(size int) {
	p.offset += size
}

func (p *Proto) Read(size int) []byte {
	value := p.packet[p.offset:p.offset + size]
	p.offset += size
	return value
}

func (p *Proto) Get_lenenc_int() int{
	size := 0
	if p.packet[p.offset] < 251 {
		size = 1
	} else if p.packet[p.offset] == 252 {
		p.offset += 1
		size = 2
	} else if p.packet[p.offset] == 253 {
		p.offset += 1
		size = 3
	} else if p.packet[p.offset] == 254 {
		p.offset += 1
		size = 8
	}
	return p.Get_fixed_int(size)
}

func (p *Proto) Get_fixed_str(size int) string{
	valSlice := p.packet[p.offset:p.offset + size]
	p.offset += size
	return string(valSlice)
}

func (p *Proto) Get_null_str() string{
	value := ""
	for i := p.offset; i < len(p.packet); i++ {
		if p.packet[i] == 0x00 {
			p.offset += 1
			break
		}
		value += string(p.packet[i])
		p.offset += 1
	}
	return value
}

func (p *Proto) Get_eof_str() string{
	value := ""
	for i := p.offset; i < len(p.packet); i++ {
		if p.packet[i] == 0x00 && i == len(p.packet) - 1 {
			p.offset += 1
			break
		}
		value += string(p.packet[i])
		p.offset += 1
	}
	return value
}

func (p *Proto) Get_lenenc_str() string{
	value := ""
	size := p.Get_lenenc_int()
	for i := p.offset; i < p.offset + size; i++ {
		value += string(p.packet[i])
		p.offset += 1
	}
	return value
}


func Scramble_native_password(password, message []byte) []byte{
	if len(password) == 0{
		return nil
	}

	hash := sha1.New()
	_, err := hash.Write([]byte(password))
	if err != nil {
		logger.Error("hash password error, err: ", err.Error())
	}
	stage1 := hash.Sum(nil)

	hash.Reset()
	_, err = hash.Write(stage1)
	if err != nil {
		logger.Error("hash password error, err: ", err.Error())
	}
	stage2 := hash.Sum(nil)

	hash.Reset()

	hash.Write([]byte(message[:SCRAMBLE_LENGTH]))
	hash.Write(stage2)
	scramble := hash.Sum(nil)

	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

