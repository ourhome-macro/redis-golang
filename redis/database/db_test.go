package database

import (
	"MiddlewareSelf/redis/aof"
	"context"
	"os"
	"strings"
	"testing"
)

func TestAOFReplayKeepsDatabaseIndex(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("shared"), []byte("db0")}); err != nil {
		t.Fatalf("set db0 failed: %v", err)
	}
	if _, err := db.Exec(1, [][]byte{[]byte("SET"), []byte("shared"), []byte("db1")}); err != nil {
		t.Fatalf("set db1 failed: %v", err)
	}

	db.Close()

	reloaded := MakeDbs()
	defer reloaded.Close()

	assertBulkValue(t, reloaded, 0, "shared", "db0")
	assertBulkValue(t, reloaded, 1, "shared", "db1")
}

func TestRewriteKeepsDatabaseIndex(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("k0"), []byte("v0")}); err != nil {
		t.Fatalf("set db0 failed: %v", err)
	}
	if _, err := db.Exec(2, [][]byte{[]byte("SET"), []byte("k2"), []byte("v2")}); err != nil {
		t.Fatalf("set db2 failed: %v", err)
	}

	if err := db.RewriteAOF(context.Background()); err != nil {
		t.Fatalf("RewriteAOF failed: %v", err)
	}

	db.Close()

	reloaded := MakeDbs()
	defer reloaded.Close()

	assertBulkValue(t, reloaded, 0, "k0", "v0")
	assertBulkValue(t, reloaded, 2, "k2", "v2")
}

func TestExecMutatesBeforeAppendFails(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}
	defer db.Close()

	if err := db.aof.File.Close(); err != nil {
		t.Fatalf("close aof file failed: %v", err)
	}

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("k"), []byte("v")}); err == nil {
		t.Fatal("expected Exec to fail when AOF append fails")
	}

	reply, err := db.Exec(0, [][]byte{[]byte("GET"), []byte("k")})
	if err != nil {
		t.Fatalf("get after failed set returned error: %v", err)
	}
	got, ok := reply.([]byte)
	if !ok {
		t.Fatalf("expected bulk reply after failed append, got %T", reply)
	}
	if string(got) != "v" {
		t.Fatalf("expected key to stay in memory after failed append, got %q", string(got))
	}

	if db.aof.LastWriteError() == nil {
		t.Fatal("expected AOF last write status to record the failure")
	}
}

func TestInfoPersistenceReportsAOFState(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("k0"), []byte("v0")}); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if _, err := db.Exec(1, [][]byte{[]byte("SET"), []byte("k1"), []byte("v1")}); err != nil {
		t.Fatalf("set on db1 failed: %v", err)
	}
	if err := db.RewriteAOF(context.Background()); err != nil {
		t.Fatalf("RewriteAOF failed: %v", err)
	}

	reply, err := db.Exec(0, [][]byte{[]byte("INFO"), []byte("persistence")})
	if err != nil {
		t.Fatalf("INFO persistence failed: %v", err)
	}

	body, ok := reply.([]byte)
	if !ok {
		t.Fatalf("expected bulk bytes from INFO, got %T", reply)
	}
	info := parseInfo(string(body))

	if info["aof_enabled"] != "1" {
		t.Fatalf("expected aof_enabled=1, got %q", info["aof_enabled"])
	}
	if info["aof_last_write_status"] != "ok" {
		t.Fatalf("expected aof_last_write_status=ok, got %q", info["aof_last_write_status"])
	}
	if info["aof_last_bgrewrite_status"] != "ok" {
		t.Fatalf("expected aof_last_bgrewrite_status=ok, got %q", info["aof_last_bgrewrite_status"])
	}
	if info["aof_rewrite_count"] != "1" {
		t.Fatalf("expected aof_rewrite_count=1, got %q", info["aof_rewrite_count"])
	}
	if info["rdb_changes_since_last_save"] != "2" {
		t.Fatalf("expected dirty count 2, got %q", info["rdb_changes_since_last_save"])
	}
}

func assertBulkValue(t *testing.T, db *Db, index int, key, want string) {
	t.Helper()

	reply, err := db.Exec(index, [][]byte{[]byte("GET"), []byte(key)})
	if err != nil {
		t.Fatalf("GET %s on db %d failed: %v", key, index, err)
	}

	got, ok := reply.([]byte)
	if !ok {
		t.Fatalf("expected bulk reply for %s on db %d, got %T", key, index, reply)
	}
	if string(got) != want {
		t.Fatalf("GET %s on db %d expected %q, got %q", key, index, want, string(got))
	}
}

func chdirTemp(t *testing.T) {
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

func parseInfo(raw string) map[string]string {
	out := make(map[string]string)
	for _, line := range strings.Split(raw, "\r\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		out[key] = val
	}
	return out
}
