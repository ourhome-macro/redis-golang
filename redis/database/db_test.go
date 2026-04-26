package database

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/redis/datastruct"
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
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
		t.Fatalf("expected failed append to leave memory unchanged, got %q", reply)
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

func TestExpireCommands(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	defer db.Close()

	assertIntegerReply(t, db, 0, []string{"TTL", "missing"}, -2)
	assertIntegerReply(t, db, 0, []string{"PTTL", "missing"}, -2)
	assertIntegerReply(t, db, 0, []string{"EXPIRE", "missing", "10"}, 0)

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("key"), []byte("value")}); err != nil {
		t.Fatalf("SET failed: %v", err)
	}

	assertIntegerReply(t, db, 0, []string{"TTL", "key"}, -1)
	assertIntegerReply(t, db, 0, []string{"PERSIST", "key"}, 0)
	assertIntegerReply(t, db, 0, []string{"EXPIRE", "key", "10"}, 1)

	ttl := mustIntegerReply(t, db, 0, []string{"TTL", "key"})
	if ttl < 0 || ttl > 10 {
		t.Fatalf("TTL expected between 0 and 10, got %d", ttl)
	}
	pttl := mustIntegerReply(t, db, 0, []string{"PTTL", "key"})
	if pttl <= 0 || pttl > 10000 {
		t.Fatalf("PTTL expected between 1 and 10000, got %d", pttl)
	}

	assertIntegerReply(t, db, 0, []string{"PERSIST", "key"}, 1)
	assertIntegerReply(t, db, 0, []string{"TTL", "key"}, -1)
	assertIntegerReply(t, db, 0, []string{"PERSIST", "key"}, 0)

	assertIntegerReply(t, db, 0, []string{"PEXPIRE", "key", "50"}, 1)
	time.Sleep(80 * time.Millisecond)
	assertIntegerReply(t, db, 0, []string{"PTTL", "key"}, -2)

	reply, err := db.Exec(0, [][]byte{[]byte("GET"), []byte("key")})
	if err != nil {
		t.Fatalf("GET after expiration failed: %v", err)
	}
	if reply != nil {
		t.Fatalf("expected expired key to be removed, got %q", reply)
	}
}

func TestExpireReplaySurvivesRestart(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("session"), []byte("alive")}); err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if _, err := db.Exec(0, [][]byte{[]byte("PEXPIRE"), []byte("session"), []byte("300")}); err != nil {
		t.Fatalf("PEXPIRE failed: %v", err)
	}

	db.Close()
	time.Sleep(120 * time.Millisecond)

	reloaded := MakeDbs()
	defer reloaded.Close()

	pttl := mustIntegerReply(t, reloaded, 0, []string{"PTTL", "session"})
	if pttl <= 0 || pttl > 220 {
		t.Fatalf("expected replayed PTTL to keep original deadline, got %d", pttl)
	}

	assertBulkValue(t, reloaded, 0, "session", "alive")
}

func TestAOFUsesAbsoluteExpireCommands(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("session"), []byte("alive")}); err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if _, err := db.Exec(0, [][]byte{[]byte("PEXPIRE"), []byte("session"), []byte("5000")}); err != nil {
		t.Fatalf("PEXPIRE failed: %v", err)
	}
	if _, err := db.Exec(0, [][]byte{[]byte("SETWITHTTL"), []byte("cached"), []byte("value"), []byte("5000")}); err != nil {
		t.Fatalf("SETWITHTTL failed: %v", err)
	}
	db.Close()

	raw, err := os.ReadFile(aof.AofName)
	if err != nil {
		t.Fatalf("read aof failed: %v", err)
	}
	body := string(raw)
	if strings.Contains(body, "$7\r\nPEXPIRE\r\n") || strings.Contains(body, "$10\r\nSETWITHTTL\r\n") {
		t.Fatalf("expected relative expire commands to be canonicalized, got %q", body)
	}
	if !strings.Contains(body, "PEXPIREAT") {
		t.Fatalf("expected PEXPIREAT in AOF, got %q", body)
	}
	if !strings.Contains(body, "SETWITHPXAT") {
		t.Fatalf("expected SETWITHPXAT in AOF, got %q", body)
	}
}

func TestExpireRejectsOverflow(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	defer db.Close()

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("k"), []byte("v")}); err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if _, err := db.Exec(0, [][]byte{[]byte("EXPIRE"), []byte("k"), []byte(strconv.FormatInt(maxInt64, 10))}); err == nil {
		t.Fatal("expected EXPIRE overflow to fail")
	}
	if _, err := db.Exec(0, [][]byte{[]byte("PEXPIRE"), []byte("k"), []byte(strconv.FormatInt(maxInt64, 10))}); err == nil {
		t.Fatal("expected PEXPIRE overflow to fail")
	}
	if _, err := db.Exec(0, [][]byte{[]byte("PEXPIREAT"), []byte("k"), []byte(strconv.FormatInt(maxInt64, 10))}); err == nil {
		t.Fatal("expected PEXPIREAT overflow to fail")
	}
	if _, err := db.Exec(0, [][]byte{[]byte("SETWITHPXAT"), []byte("k"), []byte("v"), []byte(strconv.FormatInt(maxInt64, 10))}); err == nil {
		t.Fatal("expected SETWITHPXAT overflow to fail")
	}
}

func TestEvictionIsReplayable(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	db.dicts[0] = datastruct.MakeDictWithCapacity(8)
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("a"), []byte("11")}); err != nil {
		t.Fatalf("SET a failed: %v", err)
	}
	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("b"), []byte("22")}); err != nil {
		t.Fatalf("SET b failed: %v", err)
	}
	assertBulkValue(t, db, 0, "a", "11")
	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("c"), []byte("33")}); err != nil {
		t.Fatalf("SET c failed: %v", err)
	}

	if reply, err := db.Exec(0, [][]byte{[]byte("GET"), []byte("b")}); err != nil || reply != nil {
		t.Fatalf("expected b to be evicted, reply=%v err=%v", reply, err)
	}
	assertBulkValue(t, db, 0, "a", "11")
	assertBulkValue(t, db, 0, "c", "33")
	db.Close()

	reloaded := MakeDbs()
	defer reloaded.Close()

	if reply, err := reloaded.Exec(0, [][]byte{[]byte("GET"), []byte("b")}); err != nil || reply != nil {
		t.Fatalf("expected replayed AOF to keep b evicted, reply=%v err=%v", reply, err)
	}
	assertBulkValue(t, reloaded, 0, "a", "11")
	assertBulkValue(t, reloaded, 0, "c", "33")
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

func assertIntegerReply(t *testing.T, db *Db, index int, args []string, want int64) {
	t.Helper()
	if got := mustIntegerReply(t, db, index, args); got != want {
		t.Fatalf("%s expected %d, got %d", strings.Join(args, " "), want, got)
	}
}

func mustIntegerReply(t *testing.T, db *Db, index int, args []string) int64 {
	t.Helper()

	bargs := make([][]byte, 0, len(args))
	for _, arg := range args {
		bargs = append(bargs, []byte(arg))
	}

	reply, err := db.Exec(index, bargs)
	if err != nil {
		t.Fatalf("%s failed: %v", strings.Join(args, " "), err)
	}

	switch v := reply.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	default:
		t.Fatalf("%s expected integer reply, got %T", strings.Join(args, " "), reply)
		return 0
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
