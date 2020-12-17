package dump

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/goMySQLSemiSync/config"
	"github.com/goMySQLSemiSync/constants"
	"github.com/goMySQLSemiSync/packet"
	"github.com/goMySQLSemiSync/util"
	"github.com/wonderivan/logger"
	"io"
	"os"
	"strings"
)

type BinlogServer struct {
	masterId int                        // 主节点id
	host string                         // 主节点host
	port int							// 主节点port
	user string                         // dump user
	password string                     // dump password
	serverId int                        // dump_server serverId
	semiSync bool                       // dump server is semi sync
	serverUuid string                   // dump_server UUid
	heartbeatPeriod int                 // dump_server heartbeat to Master
	binlogName string                   // dump_server store binlog name
	binlogDir   string                  // dump_server store binlog dir
	clusterTag  string                  // dump mysql cluster tag

	gtid_mode  bool                     //是否开启gtid模式
	gtid_purged string 					//gtid_purged
}

type BinlogDumper struct {
	binlogServer *BinlogServer
	lastLogFile  string // 在启动过程中自动解析已经dump出来的binlog文件名
	lastLogPos   int64  // 在启动过程中自动解析已经dump出来的binlog pos

	lastGtid     string

	currentLogFile string // 启动后开始dump的binlog文件名
	currentLogPos  int64  // 启动后开始dump的binlog pos地址
}

func NewBinlogDumper(conf *config.Configuration) *BinlogDumper{
	masterId := conf.MasterId
	if masterId == 0 {
		logger.Fatal("the masterId for dump mysql is 0")
	}

	host := conf.Host
	if host == "" {
		logger.Fatal("the master host is empty")
	}

	port := conf.Port
	if port == 0 {
		logger.Fatal("the master port is 0")
	}

	user := conf.User
	if user == "" {
		logger.Fatal("the user for connect master is empty")
	}

	password := conf.Password
	if password == "" {
		logger.Fatal("the password for connect master is empty")
	}

	serverId := conf.ServerId
	if serverId == 0 {
		logger.Fatal("the sreverId for dump binlog server is 0")
	}

	semiSync := conf.SemiSync
	logger.Info("the dump binlog server semisync is %v", semiSync)

	serverUuid := conf.ServerUuid
	if serverUuid == "" {
		logger.Fatal("the uuid for dump binlog server is empty")
	}

	heartbeatPeriod := conf.HeartbeatPeriod
	if heartbeatPeriod == 0 {
		logger.Fatal("the heartbeatPeriod for dump binlog server is 0")
	}

	binlogName := conf.BinlogName
	if binlogName == "" {
		logger.Fatal("the binlogName for dump binlog server is empty")
	}

	clusterTag := conf.ClusterTag
	if clusterTag == "" {
		logger.Fatal("the clusterTag for dump binlog server is empty")
	}

	binlogBaseDir := conf.BinlogDir
	if binlogBaseDir == "" {
		logger.Fatal("the binlogBaseDir for dump binlog server is empty")
	}

	gtid_mode := conf.Gtid_mode
	logger.Info("the gtid mode for dump binlog server is %v", gtid_mode)

	gtid_purged := conf.Gtid_purged
	logger.Info("the gtid_purged for dump binlog server is %v", gtid_purged)

	//buffer := new(bytes.Buffer)
	//buffer.WriteString(binlogBaseDir)
	//buffer.WriteString("/")
	//buffer.WriteString(clusterTag)
	//binlogDir := buffer.String()
	binlogDir := binlogBaseDir

	binlogDumper := &BinlogDumper{
		binlogServer: &BinlogServer{
			masterId:         masterId,
			host:             host,
			port:             port,
			user:             user,
			password:         password,
			serverId:        serverId,
			semiSync:        semiSync,
			serverUuid:      serverUuid,
			heartbeatPeriod: heartbeatPeriod,
			binlogName:      binlogName,
			binlogDir:       binlogDir,
			clusterTag:      clusterTag,
			gtid_mode:       gtid_mode,
			gtid_purged:     gtid_purged,
		},
	}

	binlogDumper.lastGtid = ""

	//找到最后一个 / 当前的 binlog file
	binlogDumper.setLastLogFile()
	binlogDumper.setLastLogPos()
	binlogDumper.saveBinlogIndex()
	logger.Debug(binlogDumper)
	return binlogDumper
}

func (binlogDumper *BinlogDumper) getIndexFile() string{
	buffer := new(bytes.Buffer)
	//buffer.WriteString(binlogDumper.binlogServer.binlogDir)
	//buffer.WriteString("/")
	//buffer.WriteString(binlogDumper.binlogServer.clusterTag)
	//buffer.WriteString("/")
	buffer.WriteString(binlogDumper.binlogServer.binlogName)
	buffer.WriteString(".index")
	indexName := buffer.String()
	logger.Debug("the binlog index name: ", indexName)
	return indexName
}

/*
* 从 binlog index filename中读取最后一个binlog, 如果不存在，设置为 binlog.000001
*/
func (binlogDumper *BinlogDumper) setLastLogFile() {
	indexName := binlogDumper.getIndexFile()
	indexFile, err := os.OpenFile(binlogDumper.getAbsoluteFileName(indexName), os.O_CREATE|os.O_RDONLY, 0644)
	defer indexFile.Close()
	if err != nil {
		logger.Error("open binlog index file error, err: ", err.Error())
		panic(err)
	}
	logFileName, err := util.ReadLastLine(indexFile)
	if len(logFileName) == 0 {
		binlogDumper.lastLogFile = binlogDumper.binlogServer.binlogName + ".000001"
	} else {
		binlogDumper.lastLogFile = logFileName
	}
	binlogDumper.currentLogFile = binlogDumper.lastLogFile
}

/*
   last log pos对应
   binlog文件格式
   https://dev.mysql.com/doc/internals/en/event-structure.html
==========+=====================================+
          |filehead| fileheader        0 : 4    |
==========+=====================================+
 event    | event  | timestamp         0 : 4    |
          | header +----------------------------+
          |        | type_code         4 : 1    |
          |        +----------------------------+
          |        | server_id         5 : 4    |
          |        +----------------------------+
          |        | event_length      9 : 4    |
          |        +----------------------------+
          |        | next_position    13 : 4    |
          |        +----------------------------+
          |        | flags            17 : 2    |
          |        +----------------------------+
          |        | extra_headers    19 : x-19 |
          +=====================================+
          | event  | fixed part        x : y    |
          | data   +----------------------------+
          |        | variable part              |
==========+=====================================+
 event
==========+=====================================+
 event
*/
func (binlogDumper *BinlogDumper) setLastLogPos() {
	lastLogFileAbsolate := binlogDumper.getAbsoluteFileName(binlogDumper.lastLogFile)
	_, err := os.Stat(lastLogFileAbsolate)
	fileHeaderPos := int64(4)
	if err != nil {
		logger.Debug("has no binlog before, now start the first parse!")
		binlogDumper.lastLogPos = fileHeaderPos
	} else {
		logger.Debug("Parse last log pos from ", lastLogFileAbsolate)
		binlogDumper.lastLogPos = fileHeaderPos
		lastLogFile, err := os.OpenFile(lastLogFileAbsolate, os.O_RDONLY, 0444)
		defer lastLogFile.Close()
		if err != nil {
			logger.Error("open last binlog file error, err: ", err.Error())
			panic(err)
		}
		lastLogFile.Seek(fileHeaderPos, io.SeekCurrent)
		for {
			eventHeaderTimestampDescLen := 4
			typeCodeDescLen := 1
			serverIdDescLen := 4
			eventLengthDescLen := 4
			nextPositionDescLen := 4
			flagsDescLen := 2

			everyEventHead := eventHeaderTimestampDescLen + typeCodeDescLen + serverIdDescLen
			everyEventHeadSli := make([]byte, everyEventHead)
			length, err := lastLogFile.Read(everyEventHeadSli)
			if err != nil && err == io.EOF{
				logger.Debug("the last log file read done.")
				break
			}
			if length < everyEventHead {
				logger.Debug("read event done.")
				break
			}

			eventLengthBinarySlice := make([]byte, eventLengthDescLen)
			length, err = lastLogFile.Read(eventLengthBinarySlice)
			if err != nil && err == io.EOF{
				logger.Debug("the last log file read done.")
				break
			}
			if length < eventLengthDescLen {
				logger.Debug("read event done.")
				break
			}
			eventLength := binary.LittleEndian.Uint32(eventLengthBinarySlice)

			nextPositionBinarySlice := make([]byte, nextPositionDescLen)
			length, err = lastLogFile.Read(nextPositionBinarySlice)
			if err != nil && err == io.EOF{
				logger.Debug("the last log file read done.")
				break
			}
			if length < nextPositionDescLen {
				logger.Debug("read event done.")
				break
			}

			nextPosition := binary.LittleEndian.Uint32(nextPositionBinarySlice)

			length, err = lastLogFile.Read(make([]byte, flagsDescLen))
			eventDataDescLen := eventLength - 19
			length, err = lastLogFile.Read(make([]byte, eventDataDescLen))

			binlogDumper.lastLogPos = int64(nextPosition)
		}
	}
	binlogDumper.currentLogPos = binlogDumper.lastLogPos
}

func (binlogDumper *BinlogDumper) getAbsoluteFileName(filename string) string {
	return fmt.Sprintf("%s/%s", binlogDumper.binlogServer.binlogDir, filename)
}

func (binlogDumper *BinlogDumper) saveBinlogIndex() {
	indexFileName := binlogDumper.getIndexFile()
	indexFile, err := os.OpenFile(binlogDumper.getAbsoluteFileName(indexFileName), os.O_CREATE|os.O_RDWR, 0644)
	defer indexFile.Close()
	if err != nil {
		logger.Fatal("open index file error, err: ", err.Error())
	}
	lastLogFileInIndex, err := util.ReadLastLine(indexFile)
	if err != nil {
		logger.Fatal("read last log file in index file error, err:", err.Error())
	}
	if lastLogFileInIndex == binlogDumper.lastLogFile {
		return
	} else {
		_, err = indexFile.WriteString(fmt.Sprintf("%s\n", binlogDumper.lastLogFile))
		if err != nil {
			logger.Debug("err: ", err.Error())
		}
	}
}

func (binlogDumper *BinlogDumper) initBinlogFileByFileName(filename string) *os.File{
	logger.Debug(filename)
	if filename != "" {
		binlogDumper.lastLogFile = filename
		binlogDumper.currentLogFile = filename
	}
	_, err := os.Stat(filename)
	if err != nil && os.IsNotExist(err) {
		logger.Debug("the log file does not exist, will creat it")
		curLogFile, err := os.OpenFile(binlogDumper.getAbsoluteFileName(filename), os.O_CREATE | os.O_APPEND | os.O_RDWR, 0644)
		if err != nil {
			logger.Error("create cur binlog file error, file:", filename, ", err:", err.Error())
		}
		//fileHeaderBytes := []byte("abcd")
		//curLogFile.Write(fileHeaderBytes)
		fileHeaderBytes, _ := hex.DecodeString("fe62696e")
		curLogFile.Write(fileHeaderBytes)
		return curLogFile
	} else {
		logger.Debug("the file has exists, now append data")
		curLogFile, err := os.OpenFile(binlogDumper.getAbsoluteFileName(filename), os.O_APPEND | os.O_RDWR, 0644)
		if err != nil {
			logger.Error("open cur binlog file error, err:", err.Error())
		}
		return curLogFile
	}
}

func (binlogDumper *BinlogDumper) initBinlogFile() *os.File{
	return 	binlogDumper.initBinlogFileByFileName(binlogDumper.currentLogFile)
}

func (binlogDumper *BinlogDumper) SaveGtidSets(gtid string) {
	binlogDumper.lastGtid = gtid
}

func (binlogDumper *BinlogDumper) GetRotateLogFile(packetSlice []byte) string {
	var buffer bytes.Buffer
	for i := 27; i < len(packetSlice); i++ {
		if packetSlice[i] == 0x00 {
			break
		}
		buffer.WriteByte(packetSlice[i])
	}
	return buffer.String()
}

func (this *BinlogDumper) Run() {
	fw := this.initBinlogFile()
	skip_a_rotate_event := false
	auto_position := false
	if this.binlogServer.gtid_mode == true {
		auto_position = true
	}
	logger.Debug("currentLogFile: ", this.currentLogFile, ", currentLogPos: ", this.currentLogPos)
	binlogReader := newBinlogReaderStream(NewBaseStream(this.binlogServer), this.currentLogFile, this.currentLogPos, auto_position)
	for {
		timestamp, event_type, event_size, log_pos, packetSlice := binlogReader.Fetchone()
		logger.Debug("now received event[%s]:[%s] %s %s", timestamp, event_type, event_size, log_pos)

		if skip_a_rotate_event && event_type == constants.ROTATE_EVENT {
			skip_a_rotate_event = false
			continue
		}

		err := this.SaveBinlogIntoBinlogFile(fw, packetSlice)
		if err != nil {
			os.Exit(1)
		} else {
			this.SaveBinlogIntoMySQL(packetSlice)
		}

		if event_type == constants.GTID_LOG_EVENT {
			gtid_event := packet.NewGtidEvent()
			gtid_event.LoadFromPacket(packetSlice[19:])
			this.SaveGtidSets(gtid_event.GetGtid())
		}

		if event_type == constants.ROTATE_EVENT {
			fw.Close()
			newLogFile := this.GetRotateLogFile(packetSlice)
			logger.Info("Rotate new binlog file: ", newLogFile)
			this.SaveGtidIndex()
			fw = this.initBinlogFileByFileName(newLogFile)
			this.saveBinlogIndex()
			skip_a_rotate_event = true
		}
	}
}

func (this *BinlogDumper) SaveGtidIndex() {
	if this.lastGtid == "" {
		return
	}
	gtidIndexFileName := strings.Replace(this.getAbsoluteFileName(this.getIndexFile()), "index", "gtid.index", 1)
	file, err := os.OpenFile(gtidIndexFileName, os.O_CREATE | os.O_APPEND | os.O_RDWR, 0644)
	if err != nil {
		logger.Error("open file ", gtidIndexFileName, " error, err: ", err.Error())
		os.Exit(1)
	}
	rd := bufio.NewReader(file)
	for {
		lines, err := rd.ReadString('\n')
		if err != nil && err == io.EOF {
			break
		}
		if strings.Contains(lines, this.getIndexFile()) {
			return
		}
	}
	file.WriteString(fmt.Sprintf("%s:%s\n", this.lastLogFile, this.lastGtid))
}

func (this *BinlogDumper) SaveBinlogIntoBinlogFile(fw *os.File, packetSlice []byte) error{
	_, err := fw.Write(packetSlice)
	if err != nil {
		logger.Error("write packetSlice to file error, err: ", err.Error())
		return err
	}
	fw.Sync()
	return nil
}