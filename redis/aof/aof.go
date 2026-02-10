package aof

import (
	"bufio"
	"os"
	"time"
)

type SyncPolicy int

const (
	SyncAlways   SyncPolicy = iota // 每次写都同步
	SyncEverySec                   // 每秒同步
	SyncNo                         // 由操作系统决定
)

type AOF struct {
	File        *os.File
	fileName    string
	bufWriter   *bufio.Writer
	rewriteChan chan struct{}
	stopChan    chan struct{}
	syncPolicy  SyncPolicy
}

func NewAOF(fileName string, policy SyncPolicy) (*AOF, error) {
	//开启读写追加文件
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	//var fd *os.File
	if err != nil {
		f, err = os.Create(fileName)
		if err != nil {
			return nil, err
		}
	}
	aof := &AOF{
		File:        f,
		fileName:    fileName,
		rewriteChan: make(chan struct{}),
		stopChan:    make(chan struct{}),
		bufWriter:   bufio.NewWriter(f),
		syncPolicy:  policy,
	}
	if policy == SyncEverySec {
		go aof.syncLoop()
	}
	return aof, nil
}

func (aof *AOF) syncLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-aof.stopChan:
			return
		case <-ticker.C:
			_ = aof.bufWriter.Flush()
			_ = aof.File.Sync()
		}
	}
}

func (aof *AOF) Close() {
	if aof.syncPolicy == SyncEverySec {
		close(aof.stopChan)
		time.Sleep(100 * time.Millisecond)
	}
	if aof.File != nil {
		_ = aof.File.Close()
	}
	if aof.bufWriter != nil {
		_ = aof.bufWriter.Flush()
	}
}

func IsWriteCmd(cmd string) bool {
	switch cmd {
	case "SET", "DEL", "HSET", "LPUSH", "SADD", "EXPIRE", "SETWITHTTL":
		return true
	}
	return false
}
