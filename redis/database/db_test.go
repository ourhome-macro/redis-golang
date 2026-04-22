package database

import (
	"MiddlewareSelf/redis/aof"
	"context"
	"os"
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

func TestExecDoesNotMutateWhenAppendFails(t *testing.T) {
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
	if reply != nil {
		t.Fatalf("key should not exist after failed append, got %q", reply)
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
