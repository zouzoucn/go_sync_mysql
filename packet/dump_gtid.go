package packet

import (
	"bytes"
	"encoding/binary"
	"github.com/goMySQLSemiSync/protocol"
	"strings"
)

type DumpGtid struct {
	*protocol.Packet
	serverId int
	auto_position bool
	gtidSet *protocol.GtidSet
}

func (d *DumpGtid) GetPacket() *protocol.Packet {
	return d.Packet
}

func (d *DumpGtid) SetPacket(packet *protocol.Packet) {
	d.Packet = packet
}

func (d *DumpGtid) GetServerId() int {
	return d.serverId
}

func (d *DumpGtid) SetServerId(serverId int) {
	d.serverId = serverId
}

func (d *DumpGtid) GetAuto_position() bool {
	return d.auto_position
}

func (d *DumpGtid) SetAuto_position(auto_position bool) {
	d.auto_position = auto_position
}

func (d *DumpGtid) SetGtidSet(gtidSet *protocol.GtidSet) {
	d.gtidSet = gtidSet
}

func NewDumpGtid() *DumpGtid{
	return &DumpGtid{
		Packet:        protocol.NewPacket(),
		serverId:      0,
		auto_position: false,
		gtidSet: protocol.NewGtidSet(),
	}
}

//# Format for mysql packet master_auto_position
//#
//# All fields are little endian
//# All fields are unsigned
//
//# Packet length   uint   4bytes
//# Packet type     byte   1byte   == 0x1e
//# Binlog flags    ushort 2bytes  == 0 (for retrocompatibilty)
//# Server id       uint   4bytes
//# binlognamesize  uint   4bytes
//# binlogname      str    Nbytes  N = binlognamesize
//#                                Zeroified
//# binlog position uint   8bytes  == 4
//# payload_size    uint   4bytes
//
//# What come next, is the payload, where the slave gtid_executed
//# is sent to the master
//# n_sid           ulong  8bytes  == which size is the gtid_set
//# | sid           uuid   16bytes UUID as a binary
//# | n_intervals   ulong  8bytes  == how many intervals are sent
//# |                                 for this gtid
//# | | start       ulong  8bytes  Start position of this interval
//# | | stop        ulong  8bytes  Stop position of this interval
//
//# A gtid set looks like:
//#   19d69c1e-ae97-4b8c-a1ef-9e12ba966457:1-3:8-10,
//#   1c2aad49-ae92-409a-b4df-d05a03e4702e:42-47:80-100:130-140
//#
//# In this particular gtid set,
//# 19d69c1e-ae97-4b8c-a1ef-9e12ba966457:1-3:8-10
//# is the first member of the set, it is called a gtid.
//# In this gtid, 19d69c1e-ae97-4b8c-a1ef-9e12ba966457 is the sid
//# and have two intervals, 1-3 and 8-10, 1 is the start position of
//# the first interval 3 is the stop position of the first interval.
func (d *DumpGtid) GetPayload() []byte {
	gtidSet := d.gtidSet
	encoded_data_size := gtidSet.EncodeLength()
	header_size := (1 +
		2 +    //binlog_flags
		4 +					//server_id
		4 +					//binlog_name_info_size
		16 +					//empty binlog name
		8 +					//binlog_pos_info_size
		4)					//encoded_data_size
	var buf bytes.Buffer
	////# Packet length   uint   4bytes
	binary.Write(&buf, binary.LittleEndian, uint32(encoded_data_size + header_size))
	//b := make([]byte, 0, 4)

	buf.WriteByte(byte(protocol.COM_BINLOG_DUMP_GTID))
	////# Packet type     byte   1byte   == 0x1e
	//binary.LittleEndian.PutUint32(b, uint32(protocol.COM_BINLOG_DUMP_GTID))
	//buf.Write(b)

	////# Binlog flags    ushort 2bytes  == 0 (for retrocompatibilty)
	flags := 0
	//flags |= 0x04
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(flags))
	buf.Write(b)

	// server_id (4 bytes)
	b = make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(d.serverId))
	buf.Write(b)

	//binlog_name_info_size (4 bytes)
	b = make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(16))
	buf.Write(b)

	//empty_binlog_name (4 bytes)
	buf.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	//binlog_pos_info (8bytes)
	b = make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(4))
	buf.Write(b)

	//payload_size (4bytes)
	b = make([]byte, 4)
	//binary.LittleEndian.PutUint32(b, uint32(gtidSet.EncodeLength()))
	binary.LittleEndian.PutUint32(b, uint32(gtidSet.EncodeLength()))
	buf.Write(b)

	//encoded_data
	buf.Write(gtidSet.Encoded())

	return  buf.Bytes()
}

//func (d *DumpGtid) GetPayload() []byte {
//	gtidData := d.gtidSet.Encoded()
//
//	data := make([]byte, 4+1+2+4+4+len("")+8+4+len(gtidData))
//	pos := 4
//	data[pos] = byte(protocol.COM_BINLOG_DUMP_GTID)
//	pos++
//
//	binary.LittleEndian.PutUint16(data[pos:], 0)
//	pos += 2
//
//	binary.LittleEndian.PutUint32(data[pos:], uint32(d.serverId))
//	pos += 4
//
//	binary.LittleEndian.PutUint32(data[pos:], uint32(len("")))
//	pos += 4
//
//	n := copy(data[pos:], "")
//	pos += n
//
//	binary.LittleEndian.PutUint64(data[pos:], uint64(4))
//	pos += 8
//
//	binary.LittleEndian.PutUint32(data[pos:], uint32(len(gtidData)))
//	pos += 4
//	n = copy(data[pos:], gtidData)
//	pos += n
//
//	data = data[0:pos]
//	return data
//}

func (this *DumpGtid) GetPurgedGtidSet(gtid_purged string) *protocol.GtidSet{
	gtidSet := protocol.NewGtidSet()
	if gtid_purged == "" {
		return gtidSet
	}

	purged_gtid_slice := strings.Split(gtid_purged, ",")
	for _, gtidStr := range purged_gtid_slice {
		gtid := protocol.Parse(gtidStr)
		gtidSet.Add(gtid)
	}
	return gtidSet
}