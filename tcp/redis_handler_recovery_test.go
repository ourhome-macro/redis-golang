package tcp

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/redis/client"
	"MiddlewareSelf/redis/database"
	_interface "MiddlewareSelf/redis/interface"
	"MiddlewareSelf/redis/resp"
	"context"
	"net"
	"testing"
	"time"
)

func TestRedisHandlerMSetMGetPipelineEndToEnd(t *testing.T) {
	chdirTempForTCP(t)

	db := database.MakeDbs()
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}
	handler := MakeRedisHandler(db)
	addr, stop := startRecoveryRedisServer(t, handler)
	defer stop()

	cli, err := client.DialPipeline(addr, time.Second)
	if err != nil {
		t.Fatalf("DialPipeline failed: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	results, err := cli.ExecStream(ctx, []client.Command{
		client.NewCommand("MSET", "a", "1", "b", "2"),
		client.NewCommand("MGET", "a", "missing", "b"),
		client.NewCommand("GET", "a"),
	})
	if err != nil {
		t.Fatalf("ExecStream failed: %v", err)
	}

	replies := collectPipelineReplies(t, results, 3)
	if got := string(replies[0].ToBytes()); got != "+OK\r\n" {
		t.Fatalf("unexpected MSET reply: %q", got)
	}

	mget, ok := replies[1].(*resp.ArrayReply)
	if !ok {
		t.Fatalf("expected MGET array reply, got %T", replies[1])
	}
	if len(mget.Args) != 3 {
		t.Fatalf("expected 3 MGET elements, got %d", len(mget.Args))
	}
	if string(mget.Args[0]) != "1" || mget.Args[1] != nil || string(mget.Args[2]) != "2" {
		t.Fatalf("unexpected MGET elements: %#v", mget.Args)
	}
	if got := string(replies[2].ToBytes()); got != "$1\r\n1\r\n" {
		t.Fatalf("unexpected GET reply: %q", got)
	}
}

func startRecoveryRedisServer(t *testing.T, handler *RedisHandler) (string, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	closeCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ListenAndServe(listener, handler, closeCh)
	}()

	stopped := false
	return listener.Addr().String(), func() {
		if stopped {
			return
		}
		stopped = true
		close(closeCh)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("server did not stop")
		}
	}
}

func collectPipelineReplies(t *testing.T, results <-chan client.PipelineResult, want int) []_interface.Reply {
	t.Helper()

	replies := make([]_interface.Reply, 0, want)
	for result := range results {
		if result.Err != nil {
			t.Fatalf("pipeline response #%d failed: %v", result.ResponseIndex, result.Err)
		}
		replies = append(replies, result.Reply)
	}
	if len(replies) != want {
		t.Fatalf("expected %d replies, got %d", want, len(replies))
	}
	return replies
}
