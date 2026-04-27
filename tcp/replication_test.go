package tcp

import (
	"MiddlewareSelf/redis/database"
	"MiddlewareSelf/redis/parser"
	"MiddlewareSelf/redis/resp"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestReplicationFullSyncAckAndInfo(t *testing.T) {
	chdirTempForTCP(t)

	db := database.MakeDbs()
	defer db.Close()

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("alpha"), []byte("1")}); err != nil {
		t.Fatalf("seed db0 failed: %v", err)
	}
	if _, err := db.Exec(2, [][]byte{[]byte("SET"), []byte("beta"), []byte("2")}); err != nil {
		t.Fatalf("seed db2 failed: %v", err)
	}

	addr, stop := startRecoveryRedisServer(t, MakeRedisHandler(db))
	defer stop()

	conn, reader, replID, baseOffset, snapshot := openReplicaAndFullSync(t, addr, 6380)
	defer conn.Close()

	snapshotCommands := parseReplicationStream(t, snapshot)
	assertCommandSequence(t, snapshotCommands, [][]string{
		{"SELECT", "0"},
		{"SET", "alpha", "1"},
		{"SELECT", "2"},
		{"SET", "beta", "2"},
	})

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("live"), []byte("3")}); err != nil {
		t.Fatalf("live write failed: %v", err)
	}

	liveRaw, liveCommands := readCommandFrames(t, reader, 2)
	assertCommandSequence(t, liveCommands, [][]string{
		{"SELECT", "0"},
		{"SET", "live", "3"},
	})

	ackOffset := baseOffset + int64(len(liveRaw))
	sendCommand(t, conn, "REPLCONF", "ACK", strconv.FormatInt(ackOffset, 10))

	waitForReplicationInfo(t, db, func(info map[string]string, raw string) bool {
		return info["connected_slaves"] == "1" &&
			info["master_replid"] == replID &&
			info["master_repl_offset"] == strconv.FormatInt(ackOffset, 10) &&
			strings.Contains(raw, fmt.Sprintf("slave0:ip=127.0.0.1,port=6380,state=online,offset=%d", ackOffset))
	})
}

func TestReplicationPartialResyncAfterReconnect(t *testing.T) {
	chdirTempForTCP(t)

	db := database.MakeDbs()
	defer db.Close()

	addr, stop := startRecoveryRedisServer(t, MakeRedisHandler(db))
	defer stop()

	conn, reader, replID, baseOffset, _ := openReplicaAndFullSync(t, addr, 6380)

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("a"), []byte("1")}); err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	firstRaw, firstCommands := readCommandFrames(t, reader, 2)
	assertCommandSequence(t, firstCommands, [][]string{
		{"SELECT", "0"},
		{"SET", "a", "1"},
	})
	firstOffset := baseOffset + int64(len(firstRaw))
	sendCommand(t, conn, "REPLCONF", "ACK", strconv.FormatInt(firstOffset, 10))
	_ = conn.Close()

	waitForReplicationInfo(t, db, func(info map[string]string, _ string) bool {
		return info["connected_slaves"] == "0"
	})

	if _, err := db.Exec(0, [][]byte{[]byte("SET"), []byte("b"), []byte("2")}); err != nil {
		t.Fatalf("write b failed: %v", err)
	}
	if _, err := db.Exec(1, [][]byte{[]byte("SET"), []byte("c"), []byte("3")}); err != nil {
		t.Fatalf("write c failed: %v", err)
	}

	conn2, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("reconnect failed: %v", err)
	}
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)

	mustStatusReply(t, conn2, reader2, "REPLCONF", "listening-port", "6381")
	mustStatusReply(t, conn2, reader2, "REPLCONF", "capa", "psync2")
	sendCommand(t, conn2, "PSYNC", replID, strconv.FormatInt(firstOffset, 10))

	status := readSimpleLine(t, reader2)
	if status != "+CONTINUE" {
		t.Fatalf("expected +CONTINUE, got %q", status)
	}

	resyncRaw, resyncCommands := readCommandFrames(t, reader2, 4)
	assertCommandSequence(t, resyncCommands, [][]string{
		{"SELECT", "0"},
		{"SET", "b", "2"},
		{"SELECT", "1"},
		{"SET", "c", "3"},
	})

	ackOffset := firstOffset + int64(len(resyncRaw))
	sendCommand(t, conn2, "REPLCONF", "ACK", strconv.FormatInt(ackOffset, 10))

	waitForReplicationInfo(t, db, func(info map[string]string, raw string) bool {
		return info["connected_slaves"] == "1" &&
			info["master_replid"] == replID &&
			info["master_repl_offset"] == strconv.FormatInt(ackOffset, 10) &&
			strings.Contains(raw, fmt.Sprintf("slave0:ip=127.0.0.1,port=6381,state=online,offset=%d", ackOffset))
	})
}

func openReplicaAndFullSync(t *testing.T, addr string, port int) (net.Conn, *bufio.Reader, string, int64, []byte) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial replica failed: %v", err)
	}
	reader := bufio.NewReader(conn)

	mustStatusReply(t, conn, reader, "REPLCONF", "listening-port", strconv.Itoa(port))
	mustStatusReply(t, conn, reader, "REPLCONF", "capa", "psync2")
	sendCommand(t, conn, "PSYNC", "?", "-1")

	status := readSimpleLine(t, reader)
	replID, offset := parseFullResync(t, status)
	snapshot := readBulkPayload(t, reader)
	return conn, reader, replID, offset, snapshot
}

func mustStatusReply(t *testing.T, conn net.Conn, reader *bufio.Reader, args ...string) {
	t.Helper()
	sendCommand(t, conn, args...)
	if got := readSimpleLine(t, reader); got != "+OK" {
		t.Fatalf("%s reply = %q, want +OK", strings.Join(args, " "), got)
	}
}

func sendCommand(t *testing.T, conn net.Conn, args ...string) {
	t.Helper()

	command := make([][]byte, 0, len(args))
	for _, arg := range args {
		command = append(command, []byte(arg))
	}
	if _, err := conn.Write(resp.MakeArrayReply(command).ToBytes()); err != nil {
		t.Fatalf("write %s failed: %v", strings.Join(args, " "), err)
	}
}

func readSimpleLine(t *testing.T, reader *bufio.Reader) string {
	t.Helper()

	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read line failed: %v", err)
	}
	if !strings.HasSuffix(line, "\r\n") {
		t.Fatalf("line missing CRLF: %q", line)
	}
	return strings.TrimSuffix(line, "\r\n")
}

func parseFullResync(t *testing.T, status string) (string, int64) {
	t.Helper()

	parts := strings.Split(status, " ")
	if len(parts) != 3 || parts[0] != "+FULLRESYNC" {
		t.Fatalf("unexpected FULLRESYNC status: %q", status)
	}
	offset, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		t.Fatalf("parse FULLRESYNC offset failed: %v", err)
	}
	return parts[1], offset
}

func readBulkPayload(t *testing.T, reader *bufio.Reader) []byte {
	t.Helper()

	header := readSimpleLine(t, reader)
	if !strings.HasPrefix(header, "$") {
		t.Fatalf("expected bulk header, got %q", header)
	}
	length, err := strconv.Atoi(strings.TrimPrefix(header, "$"))
	if err != nil {
		t.Fatalf("parse bulk length failed: %v", err)
	}
	body := make([]byte, length+2)
	if _, err := io.ReadFull(reader, body); err != nil {
		t.Fatalf("read bulk body failed: %v", err)
	}
	if !bytes.HasSuffix(body, []byte(resp.CRLF)) {
		t.Fatalf("bulk body missing CRLF: %q", body)
	}
	return body[:length]
}

func readCommandFrames(t *testing.T, reader *bufio.Reader, count int) ([]byte, [][]string) {
	t.Helper()

	var raw []byte
	commands := make([][]string, 0, count)
	for i := 0; i < count; i++ {
		frameRaw, args := readCommandFrame(t, reader)
		raw = append(raw, frameRaw...)
		commands = append(commands, args)
	}
	return raw, commands
}

func readCommandFrame(t *testing.T, reader *bufio.Reader) ([]byte, []string) {
	t.Helper()

	header, rawHeader := readRawLine(t, reader)
	if len(header) == 0 || header[0] != '*' {
		t.Fatalf("expected array header, got %q", header)
	}
	count, err := strconv.Atoi(string(header[1:]))
	if err != nil {
		t.Fatalf("parse array length failed: %v", err)
	}

	raw := append([]byte(nil), rawHeader...)
	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		bulkHeader, rawBulkHeader := readRawLine(t, reader)
		if len(bulkHeader) == 0 || bulkHeader[0] != '$' {
			t.Fatalf("expected bulk header, got %q", bulkHeader)
		}
		raw = append(raw, rawBulkHeader...)

		length, err := strconv.Atoi(string(bulkHeader[1:]))
		if err != nil {
			t.Fatalf("parse bulk length failed: %v", err)
		}
		body := make([]byte, length+2)
		if _, err := io.ReadFull(reader, body); err != nil {
			t.Fatalf("read bulk body failed: %v", err)
		}
		if !bytes.HasSuffix(body, []byte(resp.CRLF)) {
			t.Fatalf("bulk body missing CRLF: %q", body)
		}
		raw = append(raw, body...)
		args = append(args, string(body[:length]))
	}

	return raw, args
}

func readRawLine(t *testing.T, reader *bufio.Reader) ([]byte, []byte) {
	t.Helper()

	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read raw line failed: %v", err)
	}
	if !bytes.HasSuffix(line, []byte(resp.CRLF)) {
		t.Fatalf("raw line missing CRLF: %q", line)
	}
	return line[:len(line)-2], line
}

func parseReplicationStream(t *testing.T, payload []byte) [][]string {
	t.Helper()

	ch := parser.ParseStream(bytes.NewReader(payload))
	commands := make([][]string, 0)
	for item := range ch {
		if item == nil {
			continue
		}
		if item.Err != nil {
			if item.Err == io.EOF {
				break
			}
			t.Fatalf("parse replication stream failed: %v", item.Err)
		}
		arr, ok := item.Data.(*resp.ArrayReply)
		if !ok {
			t.Fatalf("expected array command in replication stream, got %T", item.Data)
		}
		args := make([]string, 0, len(arr.Args))
		for _, arg := range arr.Args {
			args = append(args, string(arg))
		}
		commands = append(commands, args)
	}
	return commands
}

func assertCommandSequence(t *testing.T, got [][]string, want [][]string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("command count=%d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if strings.Join(got[i], "\x00") != strings.Join(want[i], "\x00") {
			t.Fatalf("command #%d = %v, want %v", i+1, got[i], want[i])
		}
	}
}

func waitForReplicationInfo(t *testing.T, db *database.Db, condition func(info map[string]string, raw string) bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		reply, err := db.Exec(0, [][]byte{[]byte("INFO"), []byte("replication")})
		if err != nil {
			t.Fatalf("INFO replication failed: %v", err)
		}
		raw := string(reply.([]byte))
		info := parseInfo(raw)
		if condition(info, raw) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("replication info condition not met before timeout")
}

func parseInfo(raw string) map[string]string {
	out := make(map[string]string)
	for _, line := range strings.Split(raw, "\r\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		out[key] = value
	}
	return out
}
