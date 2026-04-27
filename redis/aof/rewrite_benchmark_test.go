package aof

import (
	"bytes"
	"context"
	"io"
	"log"
	"path/filepath"
	"strconv"
	"testing"
)

func BenchmarkRewrite(b *testing.B) {
	cases := []struct {
		name     string
		snapshot []RewriteCommand
	}{
		{
			name:     "snapshot_1000_set_32b",
			snapshot: benchmarkRewriteSnapshot(1000, 32),
		},
		{
			name:     "snapshot_10000_set_32b",
			snapshot: benchmarkRewriteSnapshot(10000, 32),
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			a := newBenchmarkAOF(b, SyncNo)
			a.SetSnapshotProvider(func() ([]RewriteCommand, error) {
				return tc.snapshot, nil
			})

			if err := a.Rewrite(context.Background()); err != nil {
				b.Fatalf("warmup rewrite failed: %v", err)
			}

			b.ReportAllocs()
			b.SetBytes(benchmarkRewriteBytes(tc.snapshot))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := a.Rewrite(context.Background()); err != nil {
					b.Fatalf("Rewrite failed: %v", err)
				}
			}
		})
	}
}

func benchmarkRewriteSnapshot(commands int, valueSize int) []RewriteCommand {
	snapshot := make([]RewriteCommand, 0, commands+1)
	snapshot = append(snapshot, RewriteCommand{
		Args: [][]byte{
			[]byte("SELECT"),
			[]byte("0"),
		},
	})

	value := bytes.Repeat([]byte("v"), valueSize)
	for i := 0; i < commands; i++ {
		snapshot = append(snapshot, RewriteCommand{
			Args: [][]byte{
				[]byte("SET"),
				[]byte("bench:key:" + strconv.Itoa(i)),
				value,
			},
		})
	}
	return snapshot
}

func benchmarkRewriteBytes(snapshot []RewriteCommand) int64 {
	var total int64
	for _, cmd := range snapshot {
		total += int64(len(encodeRESPCommand(cmd.Args)))
	}
	return total
}

func newBenchmarkAOF(b *testing.B, policy SyncPolicy) *AOF {
	b.Helper()
	suppressAOFBenchmarkLogs(b)

	a, err := NewAOFWithFile(policy, filepath.Join(b.TempDir(), AofName))
	if err != nil {
		b.Fatalf("NewAOFWithFile failed: %v", err)
	}

	b.Cleanup(a.Close)
	return a
}

func suppressAOFBenchmarkLogs(b *testing.B) {
	b.Helper()

	originalWriter := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() {
		log.SetOutput(originalWriter)
	})
}
