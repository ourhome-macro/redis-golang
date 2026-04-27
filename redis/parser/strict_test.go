package parser

import (
	"strings"
	"testing"
	"time"
)

func TestParseStreamRejectsLFOnlyProtocolBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name:    "simple string line",
			input:   "+OK\n",
			wantErr: "missing CRLF",
		},
		{
			name:    "bulk header",
			input:   "$4\nPING\r\n",
			wantErr: "missing CRLF",
		},
		{
			name:    "bulk body terminator",
			input:   "$4\r\nPING\n",
			wantErr: "invalid bulk parse",
		},
		{
			name:    "array header",
			input:   "*1\n$4\r\nPING\r\n",
			wantErr: "missing CRLF",
		},
		{
			name:    "array element header",
			input:   "*1\r\n$4\nPING\r\n",
			wantErr: "invalid array length",
		},
		{
			name:    "array element body terminator",
			input:   "*1\r\n$4\r\nPING\n",
			wantErr: "invalid array parse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := nextStrictPayload(t, tt.input)
			if payload == nil || payload.Err == nil {
				t.Fatalf("expected parser error containing %q, got %#v", tt.wantErr, payload)
			}
			if !strings.Contains(payload.Err.Error(), tt.wantErr) {
				t.Fatalf("expected parser error containing %q, got %q", tt.wantErr, payload.Err.Error())
			}
		})
	}
}

func nextStrictPayload(t *testing.T, input string) *Payload {
	t.Helper()

	ch := ParseStream(strings.NewReader(input))
	select {
	case payload := <-ch:
		return payload
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout waiting for parser payload")
		return nil
	}
}
