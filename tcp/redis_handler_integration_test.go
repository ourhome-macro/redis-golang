package tcp

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/redis/client"
	"MiddlewareSelf/redis/database"
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRedisHandlerEndToEndSmoke(t *testing.T) {
	chdirTempForTCP(t)

	db := database.MakeDbs()
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}
	if err := db.StartActiveExpireLoop(10*time.Millisecond, 100); err != nil {
		t.Fatalf("StartActiveExpireLoop failed: %v", err)
	}
	handler := MakeRedisHandler(db)

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

	cli, err := client.DialPipeline(listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results, err := cli.ExecStream(
		ctx,
		[]client.Command{
			client.NewCommand("PING"),
			client.NewCommand("MSET", "a", "1", "b", "2"),
			client.NewCommand("MGET", "a", "missing", "b"),
			client.NewCommand("EXISTS", "a", "missing", "a"),
			client.NewCommand("BGREWRITEAOF"),
		},
	)
	if err != nil {
		t.Fatalf("ExecStream failed: %v", err)
	}

	var replies []string
	for result := range results {
		if result.Err != nil {
			t.Fatalf("pipeline response #%d failed: %v", result.ResponseIndex, result.Err)
		}
		replies = append(replies, string(result.Reply.ToBytes()))
	}
	if len(replies) != 5 {
		t.Fatalf("expected 5 replies, got %d", len(replies))
	}
	if replies[0] != "+PONG\r\n" {
		t.Fatalf("unexpected PING reply: %q", replies[0])
	}
	if replies[1] != "+OK\r\n" {
		t.Fatalf("unexpected MSET reply: %q", replies[1])
	}
	if !strings.Contains(replies[2], "$1\r\n1\r\n") || !strings.Contains(replies[2], "$-1\r\n") || !strings.Contains(replies[2], "$1\r\n2\r\n") {
		t.Fatalf("unexpected MGET reply: %q", replies[2])
	}
	if replies[3] != ":2\r\n" {
		t.Fatalf("unexpected EXISTS reply: %q", replies[3])
	}
	if replies[4] != "+Background append only file rewriting started\r\n" {
		t.Fatalf("unexpected BGREWRITEAOF reply: %q", replies[4])
	}

	close(closeCh)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop")
	}
}

func chdirTempForTCP(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd failed: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir failed: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(wd)
	})
}
