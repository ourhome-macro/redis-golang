package database

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/redis/resp"
	"context"
	"os"
	"strings"
	"testing"
)

func TestOpenDbsFailsFastOnCorruptedAOFAfterValidPrefix(t *testing.T) {
	chdirTemp(t)

	validPrefix := resp.MakeArrayReply([][]byte{
		[]byte("SET"),
		[]byte("already-loaded"),
		[]byte("value"),
	}).ToBytes()
	corruptedTail := []byte("*2\n$3\r\nGET\r\n$14\r\nalready-loaded\r\n")

	raw := append([]byte{}, validPrefix...)
	raw = append(raw, corruptedTail...)
	if err := os.WriteFile(aof.AofName, raw, 0644); err != nil {
		t.Fatalf("write corrupted AOF failed: %v", err)
	}

	db, err := OpenDbs()
	if err == nil {
		if db != nil {
			db.Close()
		}
		t.Fatal("expected OpenDbs to fail fast on corrupted AOF")
	}
	if !strings.Contains(err.Error(), "invalid AOF payload") {
		t.Fatalf("expected invalid AOF payload error, got %v", err)
	}
}

func TestRewriteAOFRecoverySurvivesRestart(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}

	if _, err := db.Exec(0, [][]byte{
		[]byte("MSET"),
		[]byte("alpha"),
		[]byte("one"),
		[]byte("beta"),
		[]byte("two"),
	}); err != nil {
		t.Fatalf("MSET db0 failed: %v", err)
	}
	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("stale"), []byte("old")}); err != nil {
		t.Fatalf("SET stale failed: %v", err)
	}
	if _, err := db.Exec(0, [][]byte{[]byte("DEL"), []byte("stale")}); err != nil {
		t.Fatalf("DEL stale failed: %v", err)
	}
	if _, err := db.Exec(2, [][]byte{
		[]byte("MSET"),
		[]byte("tenant-alpha"),
		[]byte("db2-one"),
		[]byte("tenant-beta"),
		[]byte("db2-two"),
	}); err != nil {
		t.Fatalf("MSET db2 failed: %v", err)
	}

	if err := db.RewriteAOF(context.Background()); err != nil {
		t.Fatalf("RewriteAOF failed: %v", err)
	}
	db.Close()

	reloaded, err := OpenDbs()
	if err != nil {
		t.Fatalf("OpenDbs after rewrite failed: %v", err)
	}
	defer reloaded.Close()

	assertBulkValue(t, reloaded, 0, "alpha", "one")
	assertBulkValue(t, reloaded, 0, "beta", "two")
	assertMissingValue(t, reloaded, 0, "stale")
	assertBulkValue(t, reloaded, 2, "tenant-alpha", "db2-one")
	assertBulkValue(t, reloaded, 2, "tenant-beta", "db2-two")
}

func assertMissingValue(t *testing.T, db *Db, index int, key string) {
	t.Helper()

	reply, err := db.Exec(index, [][]byte{[]byte("GET"), []byte(key)})
	if err != nil {
		t.Fatalf("GET %s on db %d failed: %v", key, index, err)
	}
	if reply != nil {
		t.Fatalf("expected %s on db %d to be missing, got %q", key, index, reply)
	}
}
