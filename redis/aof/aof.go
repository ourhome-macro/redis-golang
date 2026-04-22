package aof

import (
	"MiddlewareSelf/redis/resp"
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	AofName         = "appendonly.aof"
	RewriteTempName = "temp.aof"
)

type SyncPolicy int

const (
	SyncAlways SyncPolicy = iota
	SyncEverySec
	SyncNo
)

type AOF struct {
	File       *os.File
	fileName   string
	bufWriter  *bufio.Writer
	stopChan   chan struct{}
	syncPolicy SyncPolicy

	mu            sync.Mutex
	rewriting     bool
	rewriteBuffer [][]byte
	currentDB     int
	rewriteDB     int
	lastWriteErr  error
	rewriteStart  time.Time
	rewriteCount  int64
	lastRewriteOK bool
	lastRewriteAt time.Time
	lastRewriteMs int64

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
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		f, err = os.Create(fileName)
		if err != nil {
			return nil, err
		}
	}

	aof := &AOF{
		File:            f,
		fileName:        fileName,
		stopChan:        make(chan struct{}),
		bufWriter:       bufio.NewWriter(f),
		syncPolicy:      policy,
		currentDB:       -1,
		rewriteDB:       -1,
		lastRewriteOK:   true,
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

type PersistenceInfo struct {
	AOFEnabled           bool
	AOFCurrentSize       int64
	AOFBaseSize          int64
	AOFBufferLength      int
	AOFRewriteInProgress bool
	AOFRewriteScheduled  bool
	AOFCurrentRewriteSec int64
	AOFLastRewriteSec    int64
	AOFLastBGRewriteOK   bool
	AOFLastWriteOK       bool
	AOFRewriteCount      int64
	Loading              bool
	ChangesSinceLastSave int64
	LastSaveUnixTime     int64
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
			if err := aof.bufWriter.Flush(); err != nil {
				aof.setLastWriteErrLocked(err)
				aof.mu.Unlock()
				continue
			}
			if err := aof.File.Sync(); err != nil {
				aof.setLastWriteErrLocked(err)
				aof.mu.Unlock()
				continue
			}
			aof.setLastWriteErrLocked(nil)
			aof.mu.Unlock()
		}
	}
}

func (aof *AOF) SetSnapshotProvider(provider SnapshotProvider) {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	aof.snapshotProvider = provider
}

// AppendCommand appends a write command to the live AOF stream.
// Like Redis, the DB selector is emitted only when the selected DB changes.
func (aof *AOF) AppendCommand(dbIndex int, args [][]byte) error {
	if len(args) == 0 {
		return fmt.Errorf("empty command args")
	}
	if dbIndex < 0 {
		return fmt.Errorf("invalid db index %d", dbIndex)
	}

	aof.mu.Lock()
	defer aof.mu.Unlock()

	encoded := aof.encodeEntryLocked(dbIndex, args, false)
	if _, err := aof.bufWriter.Write(encoded); err != nil {
		aof.setLastWriteErrLocked(err)
		return err
	}

	switch aof.syncPolicy {
	case SyncAlways:
		if err := aof.bufWriter.Flush(); err != nil {
			aof.setLastWriteErrLocked(err)
			return err
		}
		if err := aof.File.Sync(); err != nil {
			aof.setLastWriteErrLocked(err)
			return err
		}
		aof.setLastWriteErrLocked(nil)
	case SyncEverySec, SyncNo:
		aof.setLastWriteErrLocked(nil)
	}

	if aof.rewriting {
		rewriteEncoded := aof.encodeEntryLocked(dbIndex, args, true)
		cmdCopy := make([]byte, len(rewriteEncoded))
		copy(cmdCopy, rewriteEncoded)
		aof.rewriteBuffer = append(aof.rewriteBuffer, cmdCopy)
	}

	return nil
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

func (aof *AOF) LastWriteError() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.lastWriteErr
}

func (aof *AOF) PersistenceInfo() PersistenceInfo {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	info := PersistenceInfo{
		AOFEnabled:           true,
		AOFBaseSize:          aof.lastRewriteSize,
		AOFRewriteInProgress: aof.rewriting,
		AOFRewriteScheduled:  false,
		AOFLastBGRewriteOK:   aof.lastRewriteOK,
		AOFLastWriteOK:       aof.lastWriteErr == nil,
		AOFRewriteCount:      aof.rewriteCount,
	}

	if aof.bufWriter != nil {
		info.AOFBufferLength = aof.bufWriter.Buffered()
	}
	if aof.File != nil {
		if fi, err := aof.File.Stat(); err == nil {
			info.AOFCurrentSize = fi.Size() + int64(info.AOFBufferLength)
		}
	}
	if aof.rewriting && !aof.rewriteStart.IsZero() {
		info.AOFCurrentRewriteSec = int64(time.Since(aof.rewriteStart).Seconds())
	} else {
		info.AOFCurrentRewriteSec = -1
	}
	if aof.lastRewriteAt.IsZero() {
		info.AOFLastRewriteSec = -1
	} else {
		info.AOFLastRewriteSec = aof.lastRewriteMs / 1000
	}

	return info
}

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
		aof.setLastWriteErrLocked(err)
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
	case "SET", "DEL", "HSET", "LPUSH", "SADD", "EXPIRE", "PEXPIRE", "PERSIST", "SETWITHTTL":
		return true
	}
	return false
}

func encodeRESPCommand(args [][]byte) []byte {
	return resp.MakeArrayReply(args).ToBytes()
}

func (aof *AOF) encodeEntryLocked(dbIndex int, args [][]byte, forRewrite bool) []byte {
	dbTracker := &aof.currentDB
	if forRewrite {
		dbTracker = &aof.rewriteDB
	}

	writeCmd := encodeRESPCommand(args)
	if *dbTracker == dbIndex {
		return writeCmd
	}

	selectCmd := encodeRESPCommand([][]byte{
		[]byte("SELECT"),
		[]byte(strconv.Itoa(dbIndex)),
	})
	*dbTracker = dbIndex

	out := make([]byte, 0, len(selectCmd)+len(writeCmd))
	out = append(out, selectCmd...)
	out = append(out, writeCmd...)
	return out
}

func (aof *AOF) setLastWriteErrLocked(err error) {
	aof.lastWriteErr = err
}
