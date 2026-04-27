package tcp

import (
	"MiddlewareSelf/redis/database"
	"bufio"
	"net"
	"testing"
	"time"
)

func TestRedisHandlerNormalizesProtocolAndCommandErrors(t *testing.T) {
	chdirTempForTCP(t)

	db := database.MakeDbs()
	handler := MakeRedisHandler(db)
	addr, stop := startRecoveryRedisServer(t, handler)
	defer stop()

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "non-array top-level resp",
			raw:  "+OK\r\n",
			want: "-ERR protocol error: expected array command\r\n",
		},
		{
			name: "unexpected leading byte",
			raw:  "wat\r\n",
			want: "-ERR protocol error: unexpected leading byte\r\n",
		},
		{
			name: "wrong arity",
			raw:  "*1\r\n$3\r\nGET\r\n",
			want: "-ERR wrong number of arguments for 'get'\r\n",
		},
		{
			name: "select invalid integer",
			raw:  "*2\r\n$6\r\nSELECT\r\n$3\r\nabc\r\n",
			want: "-ERR value is not an integer or out of range\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := conn.Write([]byte(tt.raw)); err != nil {
				t.Fatalf("write request failed: %v", err)
			}
			got, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("read reply failed: %v", err)
			}
			if got != tt.want {
				t.Fatalf("reply = %q, want %q", got, tt.want)
			}
		})
	}
}
