package parser

import (
	"MiddlewareSelf/redis/resp"
	"bytes"
	"testing"
)

func BenchmarkParseRESP(b *testing.B) {
	cases := []struct {
		name           string
		payload        []byte
		expectedFrames int
	}{
		{
			name:           "single_set_32b",
			payload:        benchmarkRESPCommand("bench:key", 32),
			expectedFrames: 1,
		},
		{
			name:           "pipeline_64_set_32b",
			payload:        benchmarkRESPPipeline(64, 32),
			expectedFrames: 64,
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.payload)))

			for i := 0; i < b.N; i++ {
				ch := make(chan *Payload, tc.expectedFrames+1)
				parse(bytes.NewReader(tc.payload), ch)

				frames := 0
				sawEOF := false
				for payload := range ch {
					if payload == nil {
						continue
					}
					if payload.Err != nil {
						if payload.Err.Error() != "EOF" {
							b.Fatalf("unexpected parser error: %v", payload.Err)
						}
						sawEOF = true
						continue
					}
					frames++
				}

				if !sawEOF {
					b.Fatal("parser did not emit EOF")
				}
				if frames != tc.expectedFrames {
					b.Fatalf("expected %d frames, got %d", tc.expectedFrames, frames)
				}
			}
		})
	}
}

func benchmarkRESPCommand(key string, valueSize int) []byte {
	value := bytes.Repeat([]byte("v"), valueSize)
	return resp.MakeArrayReply([][]byte{
		[]byte("SET"),
		[]byte(key),
		value,
	}).ToBytes()
}

func benchmarkRESPPipeline(count int, valueSize int) []byte {
	var buf bytes.Buffer
	for i := 0; i < count; i++ {
		buf.Write(benchmarkRESPCommand("bench:key", valueSize))
	}
	return buf.Bytes()
}
