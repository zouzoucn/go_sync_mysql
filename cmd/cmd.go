package main

import (
	"github.com/goMySQLSemiSync/config"
	"github.com/goMySQLSemiSync/dump"
	"github.com/wonderivan/logger"
)

func main() {
	conf, err := config.Read("./base.config")
	if err != nil {
		logger.Fatal("read base.conf error, err: ", err.Error())
	}
	dumper := dump.NewBinlogDumper(conf)
	dumper.Run()
}
