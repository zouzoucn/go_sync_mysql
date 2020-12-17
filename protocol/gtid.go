package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

//   1:1500    1400:2800
func overlap(itvl1, itvl2 *Interval) bool{
	return itvl1.Start < itvl2.Stop &&  itvl1.Stop > itvl2.Start
}

// itvl1 contains itvl2        1 1000   2 200
func contains(itvl1, itvl2 *Interval) bool {
	return itvl2.Start >= itvl1.Start && itvl2.Stop <= itvl1.Stop
}

type IntervalSlice []*Interval

func (this IntervalSlice) Len() int {
	return len(this)
}

func (this IntervalSlice) Less(i, j int) bool {
	if this[i].Start < this[j].Start {
		return true
	} else if this[i].Start > this[j].Start {
		return false
	} else {
		return this[j].Stop < this[j].Stop
	}
}

func (this IntervalSlice) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}

func Normalize(s IntervalSlice) []*Interval {
	new := make([]*Interval, 0, 1)
	if len(s) == 0 {
		return new
	}
	sort.Sort(s)
	new = append(new, s[0])

	for i := 1; i < len(s); i++ {
		last := new[len(new) - 1]
		// [1,5] [6,10]
		if s[i].Start > last.Stop {
			new = append(new, s[i])
			continue
		} else {
			//[1,5] [3, 4]
			stop := s[i].Stop
			if last.Stop > stop {
				stop = last.Stop
			}
			new[len(new) - 1] = &Interval{last.Start, stop}
		}
	}
	return new
}

type Interval struct {
	Start int64
	Stop int64
}

func (this *Interval) String() string{
	if this.Stop == this.Start + 1 {
		return fmt.Sprintf("%d", this.Start)
	} else {
		return fmt.Sprintf("%d-%d", this.Start, this.Stop)
	}
}

type Gtid struct {
	sid string
	intervals []*Interval
}

func NewGtid() *Gtid{
	return &Gtid{
		sid:       "",
		intervals: []*Interval{},
	}
}

func (g *Gtid) SetSid(sid string) {
	g.sid = sid
}

func (g *Gtid) GetSid() string {
	return g.sid
}

func (g *Gtid) SetIntervals(intervals []*Interval) {
	for _, interval := range intervals {
		g.AddInterval(interval)
	}
}

func (g *Gtid) GetIntervals() []*Interval {
	return g.intervals
}

/*
*"1-64"   [1,64]
 */
func Parse_interval(interval string) *Interval{
	re := regexp.MustCompile("^([0-9]+)(?:-([0-9]+))?$")
	matchs := re.FindAllStringSubmatch(interval, -1)
	a, _ := strconv.ParseInt(matchs[0][1],10, 64)
	b := a
	if len(matchs[0]) >= 2 && matchs[0][2] != "" {
		b, _ = strconv.ParseInt(matchs[0][2],10, 64)
	}
	return &Interval{Start:a, Stop: b + 1}
}

/*  a8111585-297e-11eb-91d3-005056ae71c5:1-4:5:7-10
*   return a8111585-297e-11eb-91d3-005056ae71c5 [[1,4],[5,6],[7,10]]
*/
func Parse(gtid string) *Gtid{
	re := regexp.MustCompile("^([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})((?::[0-9-]+)+)$")
	matchs := re.FindAllStringSubmatch(gtid, -1)

	sid := matchs[0][1]
	intervalParsed := make([]*Interval, 0, 1)
	intervals := matchs[0][2]
	for _, interval := range strings.Split(intervals[1:], ":") {
		interval_Sp := Parse_interval(interval)
		intervalParsed = append(intervalParsed, interval_Sp)
	}
	return &Gtid{
		sid:       sid,
		intervals: intervalParsed,
	}
	//return sid, intervalParsed
}

func (g *Gtid) AddInterval(itvl *Interval) {
	// [10, 5]
	if itvl.Start > itvl.Stop {
		panic(fmt.Sprintf("gtid malformed interval %v", itvl))
	}
	// [[1:5][8:10][12:15]]     itvl [4:9]
	for _, interval := range g.intervals {
		if overlap(interval, itvl) {
			panic(fmt.Sprintf("Overlapping interval %v", itvl))
		}
	}
	g.intervals = append(g.intervals, itvl)
	g.intervals = Normalize(g.intervals)
}

func (g *Gtid) SubInterval(itvl *Interval) {
	new := make([]*Interval, 0, 1)
	// [10, 5]
	if itvl.Start > itvl.Stop {
		panic(fmt.Sprintf("gtid malformed interval %v", itvl))
	}

	flag := false
	for _, interval := range g.intervals {
		if overlap(interval, itvl) {
			flag = true
			break
		}
	}
	if !flag {
		return
	}

	g.intervals = Normalize(g.intervals)
	for _, existing := range g.intervals {
		if overlap(existing, itvl) {
			if existing.Start < itvl.Start {
				new = append(new, &Interval{
					Start: existing.Start,
					Stop:  itvl.Start,
				})
			}

			if existing.Stop > itvl.Stop {
				new = append(new, &Interval{
					Start: itvl.Stop,
					Stop:  existing.Stop,
				})
			}
		} else {
			new = append(new, existing)
		}
	}
	g.intervals = new
}

// Gtid g contains gtid other
func (g *Gtid) Contains(other *Gtid) bool {
	if g.sid != other.sid {
		return false
	}

	for _, oInterval := range other.intervals {
		vFlag := false
		for _, gInterval := range g.intervals {
			if contains(gInterval, oInterval) {
				vFlag = true
			}
		}
		if !vFlag {
			return false
		}
	}
	return true
}

func (g *Gtid) Add(other *Gtid) {
	if other.sid != g.sid {
		panic(fmt.Sprintf("attempt to merge different sid, %s != %s", other.sid, g.sid))
	}

	for _, interval := range other.intervals {
		g.AddInterval(interval)
	}
}

func (g *Gtid) Sub(other *Gtid) {
	if other.sid != g.sid {
		panic(fmt.Sprintf("attempt to sub different sid, %s != %s", other.sid, g.sid))
	}

	for _, interval := range other.intervals {
		g.SubInterval(interval)
	}
}

func (g *Gtid) Bytes() []byte {
	var buf bytes.Buffer
	buf.WriteString(g.sid)
	for _, interval := range g.intervals {
		buf.WriteString(":")
		buf.WriteString(interval.String())
	}
	return buf.Bytes()
}

func (g *Gtid) Encode() []byte {
	var buf bytes.Buffer
	buf.Write(g.DecodeSidToHex())
	n := int64(len(g.intervals))
	binary.Write(&buf, binary.LittleEndian, n)
	for _, i := range g.intervals {
		binary.Write(&buf, binary.LittleEndian, i.Start)
		binary.Write(&buf, binary.LittleEndian, i.Stop)
	}
	return buf.Bytes()
}

func (g *Gtid) DecodeSidToHex() []byte{
	buf := make([]byte, 16)
	hex.Decode(buf[0:4], []byte(g.sid[0:8]))
	hex.Decode(buf[4:6], []byte(g.sid[9:13]))
	hex.Decode(buf[6:8], []byte(g.sid[14:18]))
	hex.Decode(buf[8:10], []byte(g.sid[19:23]))
	hex.Decode(buf[10:16], []byte(g.sid[24:36]))
	return buf
}

func (g *Gtid) EncodeLength() int{
	// sid + n_intervals + stop/start * len(encode int64) * count_intervals
	return (16 + 8 + 2 * 8 * len(g.intervals))
}

func (g *Gtid) Decode(data []byte) error{
	if len(data) < 24 {
		return fmt.Errorf("invalid uuid set buffer, less 24")
	}

	pos := 0
	g.sid = string(data[0:16])
	pos += 16
	n := int64(binary.LittleEndian.Uint64(data[pos:pos + 8]))
	pos += 8
	if len(data) < int(16 * n) + pos {
		return fmt.Errorf("invalid uuid set buffer, must %d, but %d", pos + int(16 * n), len(data))
	}

	g.intervals = make([]*Interval, 0, n)
	var in *Interval
	for i := int64(0); i < n; i++ {
		in.Start = int64(binary.LittleEndian.Uint64(data[pos:pos+8]))
		pos += 8
		in.Stop = int64(binary.LittleEndian.Uint64(data[pos:pos+8]))
		pos += 8
		g.intervals = append(g.intervals, in)
	}
	return nil
}

type GtidSet struct {
	gtids []*Gtid
}

func (this *GtidSet) SetGtids(gtids []*Gtid) {
	this.gtids = gtids
}

func NewGtidSet() *GtidSet {
	return &GtidSet{
		gtids: []*Gtid{},
	}
}

func (this *GtidSet) merge_gtid(gtid *Gtid) {
	isNew := true
	for _, existing := range this.gtids {
		if existing.sid == gtid.sid {
			isNew = false
			existing.Add(gtid)
		}
	}

	if isNew {
		this.gtids = append(this.gtids, gtid)
	}
}

func (this *GtidSet) Contains(other *Gtid) bool{
	for _, gtid := range this.gtids {
		if gtid.Contains(other) {
			return true
		}
	}
	return false
}

func (this *GtidSet) Add(other *Gtid) {
	this.merge_gtid(other)
}

func (this *GtidSet) EncodeLength() int{
	// n gtids
	length := 8
	for _, existing := range this.gtids {
		length += existing.EncodeLength()
	}
	return length
}

func (this *GtidSet) Encoded() []byte{
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint64(len(this.gtids)))
	for _, gtid := range this.gtids {
		buf.Write(gtid.Encode())
	}
	return buf.Bytes()
}