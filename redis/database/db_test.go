package database

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/redis/datastruct"
	"MiddlewareSelf/redis/resp"
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

func TestOpenDbsFailsOnInvalidAOF(t *testing.T) {
	chdirTemp(t)

	if err := os.WriteFile(aof.AofName, resp.MakeSimpleReply("OK").ToBytes(), 0644); err != nil {
		t.Fatalf("write invalid aof failed: %v", err)
	}

	if _, err := OpenDbs(); err == nil {
		t.Fatal("expected invalid AOF to fail DB open")
	}
}

func TestOpenDbsDoesNotLoadLegacyAOFName(t *testing.T) {
	chdirTemp(t)

	legacy := resp.MakeArrayReply([][]byte{[]byte("SET"), []byte("legacy"), []byte("stale")}).ToBytes()
	if err := os.WriteFile("redis.aof", legacy, 0644); err != nil {
		t.Fatalf("write legacy aof failed: %v", err)
	}

	db, err := OpenDbs()
	if err != nil {
		t.Fatalf("OpenDbs failed: %v", err)
	}
	defer db.Close()

	reply, err := db.Exec(0, [][]byte{[]byte("GET"), []byte("legacy")})
	if err != nil {
		t.Fatalf("GET legacy failed: %v", err)
	}
	if reply != nil {
		t.Fatalf("expected legacy redis.aof to be ignored, got %q", reply)
	}
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

func TestInfoServerSectionReportsRuntimeFields(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	defer db.Close()

	reply, err := db.Exec(0, [][]byte{[]byte("INFO"), []byte("server")})
	if err != nil {
		t.Fatalf("INFO server failed: %v", err)
	}

	body, ok := reply.([]byte)
	if !ok {
		t.Fatalf("expected bulk bytes from INFO server, got %T", reply)
	}
	raw := string(body)
	if !strings.Contains(raw, "# Server") {
		t.Fatalf("expected INFO server to contain Server header, got %q", raw)
	}

	info := parseInfo(raw)
	if info["server_name"] != "redis-golang" {
		t.Fatalf("expected server_name=redis-golang, got %q", info["server_name"])
	}
	if info["redis_mode"] != "standalone" {
		t.Fatalf("expected redis_mode=standalone, got %q", info["redis_mode"])
	}
	if got := mustInfoInt(t, info, "process_id"); got != int64(os.Getpid()) {
		t.Fatalf("expected process_id=%d, got %d", os.Getpid(), got)
	}
	if info["go_version"] == "" {
		t.Fatal("expected go_version to be populated")
	}
	if got := mustInfoInt(t, info, "arch_bits"); got != int64(strconv.IntSize) {
		t.Fatalf("expected arch_bits=%d, got %d", strconv.IntSize, got)
	}
	if got := mustInfoInt(t, info, "db_count"); got != MaxNumber {
		t.Fatalf("expected db_count=%d, got %d", MaxNumber, got)
	}
	if got := mustInfoInt(t, info, "uptime_in_seconds"); got < 0 {
		t.Fatalf("expected uptime_in_seconds >= 0, got %d", got)
	}
}

func TestInfoReplicationSectionReportsMasterState(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	defer db.Close()

	reply, err := db.Exec(0, [][]byte{[]byte("INFO"), []byte("replication")})
	if err != nil {
		t.Fatalf("INFO replication failed: %v", err)
	}

	body, ok := reply.([]byte)
	if !ok {
		t.Fatalf("expected bulk bytes from INFO replication, got %T", reply)
	}

	info := parseInfo(string(body))
	if info["role"] != "master" {
		t.Fatalf("expected role=master, got %q", info["role"])
	}
	if info["connected_slaves"] != "0" {
		t.Fatalf("expected connected_slaves=0, got %q", info["connected_slaves"])
	}
	if info["master_replid"] == "" {
		t.Fatal("expected master_replid to be populated")
	}
	if got := mustInfoInt(t, info, "master_repl_offset"); got != 0 {
		t.Fatalf("expected master_repl_offset=0, got %d", got)
	}
	if got := mustInfoInt(t, info, "repl_backlog_active"); got != 1 {
		t.Fatalf("expected repl_backlog_active=1, got %d", got)
	}
}

func TestInfoDefaultIncludesServerSection(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	defer db.Close()

	reply, err := db.Exec(0, [][]byte{[]byte("INFO")})
	if err != nil {
		t.Fatalf("INFO default failed: %v", err)
	}

	body, ok := reply.([]byte)
	if !ok {
		t.Fatalf("expected bulk bytes from INFO, got %T", reply)
	}
	raw := string(body)
	if !strings.Contains(raw, "# Server") {
		t.Fatalf("expected INFO default to contain Server section, got %q", raw)
	}
	if !strings.Contains(raw, "# Persistence") {
		t.Fatalf("expected INFO default to contain Persistence section, got %q", raw)
	}
	if !strings.Contains(raw, "# Stats") {
		t.Fatalf("expected INFO default to contain Stats section, got %q", raw)
	}
	if !strings.Contains(raw, "# Replication") {
		t.Fatalf("expected INFO default to contain Replication section, got %q", raw)
	}
}

func TestBasicStandaloneCommandsAndMSetReplay(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}

	if reply, err := db.Exec(0, [][]byte{[]byte("PING")}); err != nil || reply != "PONG" {
		t.Fatalf("PING expected PONG, reply=%v err=%v", reply, err)
	}
	if reply, err := db.Exec(0, [][]byte{[]byte("PING"), []byte("hello")}); err != nil || string(reply.([]byte)) != "hello" {
		t.Fatalf("PING message expected bulk hello, reply=%v err=%v", reply, err)
	}
	if reply, err := db.Exec(0, [][]byte{[]byte("ECHO"), []byte("world")}); err != nil || string(reply.([]byte)) != "world" {
		t.Fatalf("ECHO expected world, reply=%v err=%v", reply, err)
	}
	if reply, err := db.Exec(0, [][]byte{[]byte("MSET"), []byte("a"), []byte("1"), []byte("b"), []byte("2")}); err != nil || reply != "OK" {
		t.Fatalf("MSET expected OK, reply=%v err=%v", reply, err)
	}
	assertIntegerReply(t, db, 0, []string{"EXISTS", "a", "missing", "a"}, 2)

	reply, err := db.Exec(0, [][]byte{[]byte("MGET"), []byte("a"), []byte("missing"), []byte("b")})
	if err != nil {
		t.Fatalf("MGET failed: %v", err)
	}
	arr, ok := reply.(*resp.ArrayReply)
	if !ok {
		t.Fatalf("MGET expected array reply, got %T", reply)
	}
	if len(arr.Args) != 3 || string(arr.Args[0]) != "1" || arr.Args[1] != nil || string(arr.Args[2]) != "2" {
		t.Fatalf("unexpected MGET args: %#v", arr.Args)
	}

	db.Close()

	reloaded := MakeDbs()
	defer reloaded.Close()
	assertBulkValue(t, reloaded, 0, "a", "1")
	assertBulkValue(t, reloaded, 0, "b", "2")
}

func TestBGRewriteAOFCommandStartsRewrite(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("k"), []byte("v")}); err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	reply, err := db.Exec(0, [][]byte{[]byte("BGREWRITEAOF")})
	if err != nil {
		t.Fatalf("BGREWRITEAOF failed: %v", err)
	}
	if reply != "Background append only file rewriting started" {
		t.Fatalf("unexpected BGREWRITEAOF reply: %v", reply)
	}

	waitUntil(t, 2*time.Second, func() bool {
		info := db.aof.PersistenceInfo()
		return info.AOFRewriteCount == 1 && !info.AOFRewriteInProgress
	})
	assertBulkValue(t, db, 0, "k", "v")
}

func TestNoOpWritesDoNotAppendAOFOrDirty(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	if err := db.EnableAOF(aof.SyncAlways); err != nil {
		t.Fatalf("EnableAOF failed: %v", err)
	}
	defer db.Close()

	assertIntegerReply(t, db, 0, []string{"EXPIRE", "missing", "10"}, 0)
	assertIntegerReply(t, db, 0, []string{"PERSIST", "missing"}, 0)
	assertIntegerReply(t, db, 0, []string{"DEL", "missing"}, 0)

	reply, err := db.Exec(0, [][]byte{[]byte("INFO"), []byte("persistence")})
	if err != nil {
		t.Fatalf("INFO persistence failed: %v", err)
	}
	info := parseInfo(string(reply.([]byte)))
	if info["rdb_changes_since_last_save"] != "0" {
		t.Fatalf("expected dirty count to stay 0, got %q", info["rdb_changes_since_last_save"])
	}

	raw, err := os.ReadFile(aof.AofName)
	if err != nil {
		t.Fatalf("read aof failed: %v", err)
	}
	if len(raw) != 0 {
		t.Fatalf("expected no-op writes not to append AOF, got %q", raw)
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

func TestActiveExpireLoopRemovesExpiredKeysAndReportsStats(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	defer db.Close()

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("temp"), []byte("value")}); err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if _, err := db.Exec(0, [][]byte{[]byte("PEXPIRE"), []byte("temp"), []byte("30")}); err != nil {
		t.Fatalf("PEXPIRE failed: %v", err)
	}
	if db.dicts[0].Len() != 1 {
		t.Fatalf("expected key to exist before active expiration, got len=%d", db.dicts[0].Len())
	}

	if err := db.StartActiveExpireLoop(10*time.Millisecond, 100); err != nil {
		t.Fatalf("StartActiveExpireLoop failed: %v", err)
	}
	waitUntil(t, time.Second, func() bool {
		return db.dicts[0].Len() == 0
	})

	reply, err := db.Exec(0, [][]byte{[]byte("INFO"), []byte("stats")})
	if err != nil {
		t.Fatalf("INFO stats failed: %v", err)
	}
	body, ok := reply.([]byte)
	if !ok {
		t.Fatalf("expected bulk bytes from INFO stats, got %T", reply)
	}
	info := parseInfo(string(body))

	if info["active_expire_running"] != "1" {
		t.Fatalf("expected active_expire_running=1, got %q", info["active_expire_running"])
	}
	if got := mustInfoInt(t, info, "active_expire_cycles"); got <= 0 {
		t.Fatalf("expected active_expire_cycles > 0, got %d", got)
	}
	if got := mustInfoInt(t, info, "active_expired_keys"); got != 1 {
		t.Fatalf("expected active_expired_keys=1, got %d", got)
	}
	if info["active_expire_interval_ms"] != "10" {
		t.Fatalf("expected active_expire_interval_ms=10, got %q", info["active_expire_interval_ms"])
	}
	if info["active_expire_limit_per_db"] != "100" {
		t.Fatalf("expected active_expire_limit_per_db=100, got %q", info["active_expire_limit_per_db"])
	}
}

func TestActiveExpireCycleHonorsLimitPerDB(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	defer db.Close()

	expireAt := time.Now().Add(20 * time.Millisecond).UnixNano()
	db.dicts[0].SetWithExpireAt("k1", NewDataObject([]byte("v1")), expireAt)
	db.dicts[0].SetWithExpireAt("k2", NewDataObject([]byte("v2")), expireAt)
	if db.dicts[0].Len() != 2 {
		t.Fatalf("expected two expired keys before cycle, got len=%d", db.dicts[0].Len())
	}
	time.Sleep(30 * time.Millisecond)

	if got := db.runActiveExpireCycle(1); got != 1 {
		t.Fatalf("expected first cycle to remove one key, got %d", got)
	}
	if db.dicts[0].Len() != 1 {
		t.Fatalf("expected one key after limited cycle, got len=%d", db.dicts[0].Len())
	}
	if got := db.runActiveExpireCycle(1); got != 1 {
		t.Fatalf("expected second cycle to remove one key, got %d", got)
	}
	if db.dicts[0].Len() != 0 {
		t.Fatalf("expected no keys after second cycle, got len=%d", db.dicts[0].Len())
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

func mustInfoInt(t *testing.T, info map[string]string, key string) int64 {
	t.Helper()

	raw, ok := info[key]
	if !ok {
		t.Fatalf("missing INFO field %s", key)
	}
	val, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse INFO field %s=%q failed: %v", key, raw, err)
	}
	return val
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
