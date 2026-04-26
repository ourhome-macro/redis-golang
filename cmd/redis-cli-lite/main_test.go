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
		"SET":         true,
		"GET":         true,
		"DEL":         true,
		"SELECT":      true,
		"SETWITHTTL":  true,
		"SETWITHPXAT": true,
		"EXPIRE":      true,
		"PEXPIRE":     true,
		"PEXPIREAT":   true,
		"TTL":         true,
		"PTTL":        true,
		"PERSIST":     true,
		"INFO":        true,
		"HELP":        true,
		"QUIT":        true,
		"EXIT":        true,
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
		"EXPIRE",
		"PEXPIRE",
		"PEXPIREAT",
		"TTL",
		"PTTL",
		"PERSIST",
		"INFO",
	} {
		if !strings.Contains(output, keyword) {
			t.Fatalf("help output missing %q", keyword)
		}
	}

	for _, keyword := range []string{
		"PING",
		"AUTH",
		"ECHO",
		"EXISTS",
		"MGET",
		"MSET",
		"BGREWRITEAOF",
	} {
		if strings.Contains(output, keyword) {
			t.Fatalf("help output advertises unsupported command %q", keyword)
		}
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
