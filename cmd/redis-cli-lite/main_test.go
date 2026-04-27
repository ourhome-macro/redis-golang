package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestCommandKeywordsMatchAdvertisedSurface(t *testing.T) {
	expected := map[string]bool{
		"PING":         true,
		"ECHO":         true,
		"SET":          true,
		"MSET":         true,
		"GET":          true,
		"MGET":         true,
		"DEL":          true,
		"EXISTS":       true,
		"SELECT":       true,
		"SETWITHTTL":   true,
		"SETWITHPXAT":  true,
		"EXPIRE":       true,
		"PEXPIRE":      true,
		"PEXPIREAT":    true,
		"TTL":          true,
		"PTTL":         true,
		"PERSIST":      true,
		"INFO":         true,
		"BGREWRITEAOF": true,
		"HELP":         true,
		"QUIT":         true,
		"EXIT":         true,
	}

	seen := make(map[string]bool, len(commandKeywords))
	for _, keyword := range commandKeywords {
		if seen[keyword] {
			t.Fatalf("duplicate command keyword %q", keyword)
		}
		seen[keyword] = true
		if !expected[keyword] {
			t.Fatalf("unexpected command keyword %q", keyword)
		}
	}

	for keyword := range expected {
		if !seen[keyword] {
			t.Fatalf("missing command keyword %q", keyword)
		}
	}
}

func TestHelpAdvertisesSupportedExpireInfoCommands(t *testing.T) {
	output := captureStdout(t, printHelp)

	for _, keyword := range []string{
		"PING",
		"ECHO",
		"EXISTS",
		"MGET",
		"MSET",
		"EXPIRE",
		"PEXPIRE",
		"PEXPIREAT",
		"TTL",
		"PTTL",
		"PERSIST",
		"INFO",
		"stats",
	} {
		if !strings.Contains(output, keyword) {
			t.Fatalf("help output missing %q", keyword)
		}
	}

	for _, keyword := range []string{
		"AUTH",
	} {
		if strings.Contains(output, keyword) {
			t.Fatalf("help output advertises unsupported command %q", keyword)
		}
	}
}

func TestFormatRESPHumanArray(t *testing.T) {
	raw := "*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n2\r\n"
	want := "1) 1\n2) (nil)\n3) 2"
	if got := formatRESPHuman(raw); got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}
	defer reader.Close()

	os.Stdout = writer
	defer func() {
		os.Stdout = oldStdout
	}()

	fn()

	if err := writer.Close(); err != nil {
		t.Fatalf("close stdout writer: %v", err)
	}

	var output bytes.Buffer
	if _, err := io.Copy(&output, reader); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	return output.String()
}
