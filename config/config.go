package config

import (
	"encoding/json"
	"fmt"
	"github.com/wonderivan/logger"
	"os"
)

type Configuration struct {
	MasterId int                        // 主节点id
	Host string                         // 主节点host
	Port int							// 主节点port
	User string                         // dump user
	Password string                     // dump password
	ServerId int                        // dump_server serverId
	SemiSync bool                       // dump server is semi sync
	ServerUuid string                   // dump_server UUid
	HeartbeatPeriod int                 // dump_server heartbeat to Master
	BinlogName string                   // dump_server store binlog name
	BinlogDir   string                  // dump_server store binlog dir
	ClusterTag  string                  // dump mysql cluster tag

	Gtid_mode  bool                     //是否开启gtid模式
	Gtid_purged string 					//gtid_purged
}

func newConfiguration() *Configuration {
	return &Configuration{
		MasterId:        0,
		Host:            "",
		Port:            0,
		User:            "",
		Password:        "",
		ServerId:        0,
		SemiSync:        false,
		ServerUuid:      "",
		HeartbeatPeriod: 0,
		BinlogName:      "",
		BinlogDir:       "",
		ClusterTag:      "",
		Gtid_mode:       false,
		Gtid_purged:     "",
	}
}

var config = newConfiguration()

func Read(filename string) (*Configuration, error) {
	if (filename == "") {
		return config, fmt.Errorf("empty config file name")
	}
	file, err := os.Open(filename)
	if err != nil {
		return config, err
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(config)
	if err == nil {
		logger.Info("Read Config %s", filename)
	} else {
		logger.Fatal("Cannot read config file, filename: %s, err: %s", filename, err.Error())
	}
	return config, err
}