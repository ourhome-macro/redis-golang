package aof

import (
	"MiddlewareSelf/redis/resp"
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

const (
	// AofName 为当前默认 AOF 文件名（对齐 Redis appendonly.aof 命名）。
	AofName = "appendonly.aof"
	// RewriteTempName 为重写阶段的临时文件。
	RewriteTempName = "temp.aof"
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
	stopChan    chan struct{}
	syncPolicy  SyncPolicy

	mu            sync.Mutex
	rewriting     bool
	rewriteBuffer [][]byte

	// snapshotProvider 由上层（DB）注入，用于提供“fork 时刻”的只读快照命令。
	snapshotProvider SnapshotProvider

	autoRewriteStop chan struct{}
	autoRewriteWG   sync.WaitGroup
	lastRewriteSize int64

	closeAutoOnce sync.Once
	closeSyncOnce sync.Once
}

func NewAOF(policy SyncPolicy) (*AOF, error) {
	return NewAOFWithFile(policy, AofName)
}

func NewAOFWithFile(policy SyncPolicy, fileName string) (*AOF, error) {
	//开启读写追加文件
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		f, err = os.Create(fileName)
		if err != nil {
			return nil, err
		}
	}
	aof := &AOF{
		File:        f,
		fileName:    fileName,
		stopChan:    make(chan struct{}),
		bufWriter:   bufio.NewWriter(f),
		syncPolicy:  policy,
		rewriting:   false,
		autoRewriteStop: make(chan struct{}),
	}
	if fi, statErr := f.Stat(); statErr == nil {
		aof.lastRewriteSize = fi.Size()
	}
	if policy == SyncEverySec {
		go aof.syncLoop()
	}
	log.Printf("[AOF] opened file=%s policy=%d", fileName, policy)
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
			aof.mu.Lock()
			_ = aof.bufWriter.Flush()
			_ = aof.File.Sync()
			aof.mu.Unlock()
		}
	}
}

// SetSnapshotProvider 注入重写快照提供器。
func (aof *AOF) SetSnapshotProvider(provider SnapshotProvider) {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	aof.snapshotProvider = provider
}

// AppendCommand 以 RESP Array 格式将命令写入 AOF。
// 如果重写正在进行，会把同一份命令追加到 rewrite buffer（模拟 COW 增量收集）。
func (aof *AOF) AppendCommand(args [][]byte) error {
	if len(args) == 0 {
		return fmt.Errorf("empty command args")
	}

	encoded := encodeRESPCommand(args)

	aof.mu.Lock()
	defer aof.mu.Unlock()

	if _, err := aof.bufWriter.Write(encoded); err != nil {
		return err
	}

	switch aof.syncPolicy {
	case SyncAlways:
		if err := aof.bufWriter.Flush(); err != nil {
			return err
		}
		if err := aof.File.Sync(); err != nil {
			return err
		}
	case SyncNo, SyncEverySec:
		// SyncNo: 依赖 OS；SyncEverySec: 后台 ticker 负责 flush+sync。
	}

	if aof.rewriting {
		cmdCopy := make([]byte, len(encoded))
		copy(cmdCopy, encoded)
		aof.rewriteBuffer = append(aof.rewriteBuffer, cmdCopy)
	}

	return nil
}

func encodeRESPCommand(args [][]byte) []byte {
	return resp.MakeArrayReply(args).ToBytes()
}

func (aof *AOF) Close() {
	aof.closeAutoOnce.Do(func() {
		close(aof.autoRewriteStop)
	})
	aof.autoRewriteWG.Wait()

	if aof.syncPolicy == SyncEverySec {
		aof.closeSyncOnce.Do(func() {
			close(aof.stopChan)
		})
		time.Sleep(100 * time.Millisecond)
	}
	aof.mu.Lock()
	defer aof.mu.Unlock()

	if aof.bufWriter != nil {
		_ = aof.bufWriter.Flush()
	}
	if aof.File != nil {
		_ = aof.File.Sync()
		_ = aof.File.Close()
	}
	log.Printf("[AOF] closed file=%s", aof.fileName)
}

// StartAutoRewriteLoop 启动自动重写后台循环。
//
// 触发条件：
// 1) 当前 AOF 文件大小 >= minSizeBytes；
// 2) 相对上次重写后大小增长比例 >= growthPercent（如 100 表示增长 100%）。
func (aof *AOF) StartAutoRewriteLoop(interval time.Duration, minSizeBytes int64, growthPercent float64) {
	if interval <= 0 {
		interval = time.Second
	}

	aof.autoRewriteWG.Add(1)
	go func() {
		defer aof.autoRewriteWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-aof.autoRewriteStop:
				return
			case <-ticker.C:
				currentSize, baseline, should, err := aof.shouldAutoRewrite(minSizeBytes, growthPercent)
				if err != nil {
					log.Printf("[AOF-AUTO-REWRITE] stat failed: %v", err)
					continue
				}
				if !should {
					continue
				}

				log.Printf("[AOF-AUTO-REWRITE] trigger: current=%d baseline=%d min=%d growth=%.2f%%",
					currentSize, baseline, minSizeBytes, growthPercent)

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				err = aof.Rewrite(ctx)
				cancel()
				if err != nil {
					log.Printf("[AOF-AUTO-REWRITE] rewrite failed: %v", err)
				}
			}
		}
	}()
}

func (aof *AOF) shouldAutoRewrite(minSizeBytes int64, growthPercent float64) (currentSize int64, baseline int64, should bool, err error) {
	aof.mu.Lock()
	if err = aof.bufWriter.Flush(); err != nil {
		aof.mu.Unlock()
		return
	}
	fi, statErr := aof.File.Stat()
	if statErr != nil {
		aof.mu.Unlock()
		err = statErr
		return
	}
	currentSize = fi.Size()
	baseline = aof.lastRewriteSize
	rewriting := aof.rewriting
	aof.mu.Unlock()

	if rewriting {
		return currentSize, baseline, false, nil
	}
	if currentSize < minSizeBytes {
		return currentSize, baseline, false, nil
	}
	if baseline <= 0 {
		baseline = 1
	}

	growth := (float64(currentSize-baseline) / float64(baseline)) * 100
	should = growth >= growthPercent
	return
}

func IsWriteCmd(cmd string) bool {
	switch cmd {
	case "SET", "DEL", "HSET", "LPUSH", "SADD", "EXPIRE", "SETWITHTTL":
		return true
	}
	return false
}
