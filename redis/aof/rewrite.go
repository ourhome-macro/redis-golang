package aof

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// RewriteCommand 是重写快照中的一条逻辑命令（通常为 SET/SETWITHTTL）。
type RewriteCommand struct {
	Args [][]byte
}

// SnapshotProvider 提供“fork 时刻”的只读快照。
//
// 语义：
// - 在 Redis(C) 中，子进程通过 fork + COW 得到天然时间点快照；
// - 在本 Go 实现中，用快照回调在短临界区复制数据，再交给后台协程写 temp.aof。
type SnapshotProvider func() ([]RewriteCommand, error)

type rewriteChildResult struct {
	err error
}

// Rewrite 执行一次 AOF 重写。
//
// 时间线：
// 1) 标记 rewriting=true，开始收集 rewriteBuffer 增量命令；
// 2) 后台“子协程”读取快照并写入 temp.aof；
// 3) 子协程完成后，主协程将 rewriteBuffer 追加到 temp.aof；
// 4) 原子 rename(temp.aof -> appendonly.aof)，并重建当前 AOF 句柄。
func (aof *AOF) Rewrite(ctx context.Context) error {
	aof.mu.Lock()
	if aof.rewriting {
		aof.mu.Unlock()
		return fmt.Errorf("rewrite already in progress")
	}
	if aof.snapshotProvider == nil {
		aof.mu.Unlock()
		return fmt.Errorf("snapshot provider is not set")
	}
	aof.rewriting = true
	aof.rewriteBuffer = aof.rewriteBuffer[:0]
	aof.mu.Unlock()

	start := time.Now()
	log.Printf("[AOF-REWRITE] start, file=%s", aof.fileName)

	tmpPath := filepath.Join(filepath.Dir(aof.fileName), RewriteTempName)
	_ = os.Remove(tmpPath)
	childDone := make(chan rewriteChildResult, 1)

	go func() {
		log.Printf("[AOF-REWRITE][child] snapshot phase begin")
		snapshot, err := aof.snapshotProvider()
		if err != nil {
			childDone <- rewriteChildResult{err: fmt.Errorf("snapshot failed: %w", err)}
			return
		}

		tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			childDone <- rewriteChildResult{err: fmt.Errorf("open temp file failed: %w", err)}
			return
		}

		for i, cmd := range snapshot {
			if len(cmd.Args) == 0 {
				continue
			}
			if _, err := tmpFile.Write(encodeRESPCommand(cmd.Args)); err != nil {
				_ = tmpFile.Close()
				childDone <- rewriteChildResult{err: fmt.Errorf("write snapshot cmd #%d failed: %w", i+1, err)}
				return
			}
		}

		if err := tmpFile.Sync(); err != nil {
			_ = tmpFile.Close()
			childDone <- rewriteChildResult{err: fmt.Errorf("sync temp file failed: %w", err)}
			return
		}
		if err := tmpFile.Close(); err != nil {
			childDone <- rewriteChildResult{err: fmt.Errorf("close temp file failed: %w", err)}
			return
		}

		log.Printf("[AOF-REWRITE][child] snapshot done, cmd_count=%d", len(snapshot))
		childDone <- rewriteChildResult{}
	}()

	select {
	case <-ctx.Done():
		aof.mu.Lock()
		aof.rewriting = false
		aof.rewriteBuffer = nil
		aof.mu.Unlock()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rewrite canceled: %w", ctx.Err())
	case result := <-childDone:
		if result.err != nil {
			aof.mu.Lock()
			aof.rewriting = false
			aof.rewriteBuffer = nil
			aof.mu.Unlock()
			_ = os.Remove(tmpPath)
			return result.err
		}
	}

	aof.mu.Lock()
	defer aof.mu.Unlock()
	defer func() {
		aof.rewriting = false
		aof.rewriteBuffer = nil
	}()

	log.Printf("[AOF-REWRITE] merge incremental buffer, buffered_cmd=%d", len(aof.rewriteBuffer))
	appendFile, err := os.OpenFile(tmpPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("open temp file for append failed: %w", err)
	}
	for i, cmd := range aof.rewriteBuffer {
		if _, err := appendFile.Write(cmd); err != nil {
			_ = appendFile.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("append rewrite buffer cmd #%d failed: %w", i+1, err)
		}
	}
	if err := appendFile.Sync(); err != nil {
		_ = appendFile.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("sync merged temp file failed: %w", err)
	}
	if err := appendFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close merged temp file failed: %w", err)
	}

	// 在替换前先把当前 AOF 刷盘并关闭。
	if err := aof.bufWriter.Flush(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("flush current aof failed: %w", err)
	}
	if err := aof.File.Sync(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("sync current aof failed: %w", err)
	}
	if err := aof.File.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close current aof failed: %w", err)
	}

	if err := replaceAOFFile(tmpPath, aof.fileName); err != nil {
		// 替换失败时尽力恢复旧文件句柄，避免主线程后续写入中断。
		if reopenErr := aof.reopenWriter(); reopenErr != nil {
			return fmt.Errorf("replace failed: %v; reopen old file failed: %w", err, reopenErr)
		}
		return err
	}

	if err := aof.reopenWriter(); err != nil {
		return fmt.Errorf("reopen new aof failed: %w", err)
	}
	if fi, statErr := aof.File.Stat(); statErr == nil {
		aof.lastRewriteSize = fi.Size()
	}

	log.Printf("[AOF-REWRITE] done, cost=%s", time.Since(start))
	return nil
}

func replaceAOFFile(tmpPath, finalPath string) error {
	// 优先尝试单次 rename（在 Unix 上这是原子的）。
	if err := os.Rename(tmpPath, finalPath); err == nil {
		return nil
	}

	// 某些平台（如 Windows）目标存在时 rename 可能失败，做一次兼容回退。
	backupPath := finalPath + ".bak.rewrite"
	_ = os.Remove(backupPath)

	if err := os.Rename(finalPath, backupPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("atomic rename failed and backup move failed: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Rename(backupPath, finalPath)
		_ = os.Remove(tmpPath)
		return fmt.Errorf("replace with backup fallback failed: %w", err)
	}

	_ = os.Remove(backupPath)
	return nil
}

func (aof *AOF) reopenWriter() error {
	newFile, err := os.OpenFile(aof.fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	aof.File = newFile
	aof.bufWriter = bufio.NewWriter(newFile)
	return nil
}

