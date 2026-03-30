package aof

import (
	"MiddlewareSelf/redis/parser"
	"MiddlewareSelf/redis/resp"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"sync"
	"testing"
	"time"
)

func TestRewriteMergeIncrementalBuffer(t *testing.T) {
	dir := t.TempDir()
	aofPath := filepath.Join(dir, AofName)

	a, err := NewAOFWithFile(SyncAlways, aofPath)
	if err != nil {
		t.Fatalf("NewAOFWithFile failed: %v", err)
	}
	defer a.Close()

	state := map[string]string{"k1": "v1"}
	var mu sync.RWMutex

	a.SetSnapshotProvider(func() ([]RewriteCommand, error) {
		// 先抓取“fork 时刻”快照，再模拟子进程写盘耗时。
		mu.RLock()
		commands := make([]RewriteCommand, 0, len(state))
		for k, v := range state {
			commands = append(commands, RewriteCommand{Args: [][]byte{[]byte("SET"), []byte(k), []byte(v)}})
		}
		mu.RUnlock()

		time.Sleep(120 * time.Millisecond)
		return commands, nil
	})

	rewriteDone := make(chan error, 1)
	go func() {
		rewriteDone <- a.Rewrite(context.Background())
	}()

	// 重写窗口期间，主线程继续处理写请求。
	time.Sleep(40 * time.Millisecond)
	mu.Lock()
	state["k2"] = "v2"
	mu.Unlock()
	if err := a.AppendCommand([][]byte{[]byte("SET"), []byte("k2"), []byte("v2")}); err != nil {
		t.Fatalf("AppendCommand during rewrite failed: %v", err)
	}

	select {
	case err := <-rewriteDone:
		if err != nil {
			t.Fatalf("Rewrite failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Rewrite timeout")
	}

	cmds := readAOFCommands(t, aofPath)
	if len(cmds) != 2 {
		t.Fatalf("expected 2 commands in final aof, got %d", len(cmds))
	}

	if string(cmds[0][0]) != "SET" || string(cmds[0][1]) != "k1" || string(cmds[0][2]) != "v1" {
		t.Fatalf("unexpected first command: %q %q %q", cmds[0][0], cmds[0][1], cmds[0][2])
	}
	if string(cmds[1][0]) != "SET" || string(cmds[1][1]) != "k2" || string(cmds[1][2]) != "v2" {
		t.Fatalf("unexpected second command: %q %q %q", cmds[1][0], cmds[1][1], cmds[1][2])
	}
}

func TestRewriteFailCleanupAndRecover(t *testing.T) {
	dir := t.TempDir()
	aofPath := filepath.Join(dir, AofName)

	a, err := NewAOFWithFile(SyncAlways, aofPath)
	if err != nil {
		t.Fatalf("NewAOFWithFile failed: %v", err)
	}
	defer a.Close()

	a.SetSnapshotProvider(func() ([]RewriteCommand, error) {
		return nil, errors.New("mock snapshot error")
	})

	err = a.Rewrite(context.Background())
	if err == nil {
		t.Fatal("expected rewrite to fail")
	}

	tmpPath := filepath.Join(dir, RewriteTempName)
	if _, statErr := os.Stat(tmpPath); !os.IsNotExist(statErr) {
		t.Fatalf("temp file should be cleaned up, statErr=%v", statErr)
	}

	// 失败后仍可继续写主 AOF，证明回滚恢复可用。
	if err := a.AppendCommand([][]byte{[]byte("SET"), []byte("k"), []byte("v")}); err != nil {
		t.Fatalf("append after failed rewrite should succeed, got: %v", err)
	}

	cmds := readAOFCommands(t, aofPath)
	if len(cmds) != 1 {
		t.Fatalf("expected 1 command after append, got %d", len(cmds))
	}
}

func TestAutoRewriteLoopTrigger(t *testing.T) {
	dir := t.TempDir()
	aofPath := filepath.Join(dir, AofName)

	a, err := NewAOFWithFile(SyncAlways, aofPath)
	if err != nil {
		t.Fatalf("NewAOFWithFile failed: %v", err)
	}
	defer a.Close()

	var count int32
	a.SetSnapshotProvider(func() ([]RewriteCommand, error) {
		atomic.AddInt32(&count, 1)
		return []RewriteCommand{}, nil
	})

	a.StartAutoRewriteLoop(20*time.Millisecond, 1, 50)
	if err := a.AppendCommand([][]byte{[]byte("SET"), []byte("k"), []byte("v")}); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&count) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("auto rewrite was not triggered")
}

func readAOFCommands(t *testing.T, path string) [][][]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open aof failed: %v", err)
	}
	defer f.Close()

	ch := parser.ParseStream(f)
	out := make([][][]byte, 0)
	for payload := range ch {
		if payload == nil {
			continue
		}
		if payload.Err != nil {
			if payload.Err.Error() == "EOF" {
				break
			}
			t.Fatalf("parse aof failed: %v", payload.Err)
		}
		arr, ok := payload.Data.(*resp.ArrayReply)
		if !ok {
			t.Fatalf("unexpected payload type: %T", payload.Data)
		}
		out = append(out, arr.Args)
	}
	return out
}
