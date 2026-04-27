package database

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/redis/datastruct"
	"MiddlewareSelf/redis/resp"
	"bytes"
	"io"
	"log"
	"path/filepath"
	"testing"
)

func BenchmarkExecSet(b *testing.B) {
	args := [][]byte{
		[]byte("SET"),
		[]byte("bench:key"),
		bytes.Repeat([]byte("v"), 32),
	}

	cases := []struct {
		name    string
		withAOF bool
		policy  aof.SyncPolicy
	}{
		{name: "memory_only", withAOF: false, policy: aof.SyncNo},
		{name: "aof_sync_no", withAOF: true, policy: aof.SyncNo},
		{name: "aof_sync_always", withAOF: true, policy: aof.SyncAlways},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			db := newBenchmarkDB(b, tc.withAOF, tc.policy)

			b.ReportAllocs()
			b.SetBytes(int64(len(resp.MakeArrayReply(args).ToBytes())))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reply, err := db.Exec(0, args)
				if err != nil {
					b.Fatalf("Exec failed: %v", err)
				}
				if reply != "OK" {
					b.Fatalf("unexpected reply: %v", reply)
				}
			}
		})
	}
}

func newBenchmarkDB(b *testing.B, withAOF bool, policy aof.SyncPolicy) *Db {
	b.Helper()
	suppressBenchmarkLogs(b)

	dicts := make([]*datastruct.Dict, MaxNumber)
	for i := range dicts {
		dicts[i] = datastruct.MakeDict()
	}

	db := &Db{
		dicts:            dicts,
		activeExpireStop: make(chan struct{}),
	}

	if withAOF {
		a, err := aof.NewAOFWithFile(policy, filepath.Join(b.TempDir(), aof.AofName))
		if err != nil {
			b.Fatalf("NewAOFWithFile failed: %v", err)
		}
		db.aof = a
	}

	b.Cleanup(db.Close)
	return db
}

func suppressBenchmarkLogs(b *testing.B) {
	b.Helper()

	originalWriter := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() {
		log.SetOutput(originalWriter)
	})
}
