package util

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/wonderivan/logger"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

/*
 * 读取配置文件
*/
func ReadConfigFile(filename string) map[string]interface{} {
	var jsonData map[string]interface{}
	bytes, err := ioutil.ReadFile("./config.json")
	if err != nil {
		fmt.Println("read file config.json err: ", err.Error())
	}

	//去除注释
	configStr := string(bytes[:])
	reg := regexp.MustCompile(`/\*.*\*/`)
	configStr = reg.ReplaceAllString(configStr, "")
	bytes = []byte(configStr)
	if err = json.Unmarshal(bytes, &jsonData); err != nil {
		fmt.Println("json parse config to map error, err: ", err.Error())
		return nil
	}
	return jsonData
}

/*
* 读取文件最后一行
*/
func ReadLastLine(file *os.File) (string, error){
	buf := bufio.NewReader(file)
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		logger.Debug(line)
		if err != nil {
			if err == io.EOF {
				return line, nil
			} else {
				logger.Error("read binlog index file error, err: ", err.Error())
				return "", err
			}
		}
		return line, nil
	}
}
