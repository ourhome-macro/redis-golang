package client

import (
	"MiddlewareSelf/redis/parser"
	"MiddlewareSelf/redis/resp"
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestPipelineExecStream(t *testing.T) {
	addr, shutdown := startMockRedisServer(t, func(conn net.Conn) {
		defer conn.Close()
		ch := parser.ParseStream(conn)
		for i := 1; i <= 3; i++ {
			payload, ok := <-ch
			if !ok || payload == nil || payload.Err != nil {
				return
			}
			_, _ = conn.Write(resp.MakeSimpleReply("OK" + strconv.Itoa(i)).ToBytes())
			time.Sleep(80 * time.Millisecond)
		}
	})
	defer shutdown()

	cli, err := DialPipeline(addr, time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer cli.Close()

	cmds := []Command{
		NewCommand("PING"),
		NewCommand("ECHO", "A"),
		NewCommand("ECHO", "B"),
	}

	start := time.Now()
	resCh, err := cli.ExecStream(context.Background(), cmds)
	if err != nil {
		t.Fatalf("ExecStream failed: %v", err)
	}

	count := 0
	for res := range resCh {
		if res.Err != nil {
			t.Fatalf("unexpected error on #%d: %v", res.ResponseIndex, res.Err)
		}
		count++
	}

	if count != 3 {
		t.Fatalf("expected 3 responses, got %d", count)
	}

	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("responses returned too fast, expected streaming behavior, elapsed=%v", elapsed)
	}
}

func TestPipelineReadFailAtNthResponse(t *testing.T) {
	addr, shutdown := startMockRedisServer(t, func(conn net.Conn) {
		defer conn.Close()
		ch := parser.ParseStream(conn)

		// 读取两条命令，但只返回第一条响应，随后断开连接
		for i := 0; i < 2; i++ {
			payload, ok := <-ch
			if !ok || payload == nil || payload.Err != nil {
				return
			}
			if i == 0 {
				_, _ = conn.Write(resp.MakeSimpleReply("OK").ToBytes())
			}
		}
	})
	defer shutdown()

	cli, err := DialPipeline(addr, time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer cli.Close()

	cmds := []Command{
		NewCommand("SET", "k", "1"),
		NewCommand("GET", "k"),
	}

	resCh, err := cli.ExecStream(context.Background(), cmds)
	if err != nil {
		t.Fatalf("ExecStream failed: %v", err)
	}

	var got []PipelineResult
	for res := range resCh {
		got = append(got, res)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 results (1 success + 1 error), got %d", len(got))
	}
	if got[0].Err != nil || got[0].ResponseIndex != 1 {
		t.Fatalf("first response should succeed, got: %+v", got[0])
	}
	if got[1].Err == nil {
		t.Fatalf("second response should fail")
	}
	if got[1].ResponseIndex != 2 {
		t.Fatalf("expected failure at response #2, got #%d", got[1].ResponseIndex)
	}
	if got[1].SuccessBefore != 1 {
		t.Fatalf("expected SuccessBefore=1, got %d", got[1].SuccessBefore)
	}
}

func startMockRedisServer(t *testing.T, handler func(conn net.Conn)) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		handler(conn)
	}()

	shutdown := func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
		}
	}

	return ln.Addr().String(), shutdown
}
