package database

import "testing"

func TestCommandRegistryMetadata(t *testing.T) {
	expected := map[string]commandSpec{
		"PING":         {name: "PING", minArity: 1, maxArity: 2, argStep: 1},
		"ECHO":         {name: "ECHO", minArity: 2, maxArity: 2, argStep: 1},
		"INFO":         {name: "INFO", minArity: 1, maxArity: 2, argStep: 1},
		"SELECT":       {name: "SELECT", minArity: 2, maxArity: 2, argStep: 1},
		"BGREWRITEAOF": {name: "BGREWRITEAOF", minArity: 1, maxArity: 1, argStep: 1},

		"EXISTS": {name: "EXISTS", minArity: 2, maxArity: variableArity, argStep: 1, keyspace: true, firstKey: 1, lastKey: -1, keyStep: 1},
		"MGET":   {name: "MGET", minArity: 2, maxArity: variableArity, argStep: 1, keyspace: true, firstKey: 1, lastKey: -1, keyStep: 1},
		"GET":    {name: "GET", minArity: 2, maxArity: 2, argStep: 1, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
		"TTL":    {name: "TTL", minArity: 2, maxArity: 2, argStep: 1, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
		"PTTL":   {name: "PTTL", minArity: 2, maxArity: 2, argStep: 1, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},

		"SET":         {name: "SET", minArity: 3, maxArity: 3, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
		"MSET":        {name: "MSET", minArity: 3, maxArity: variableArity, argStep: 2, write: true, keyspace: true, firstKey: 1, lastKey: -1, keyStep: 2},
		"SETWITHTTL":  {name: "SETWITHTTL", minArity: 4, maxArity: 4, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
		"SETWITHPXAT": {name: "SETWITHPXAT", minArity: 4, maxArity: 4, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
		"EXPIRE":      {name: "EXPIRE", minArity: 3, maxArity: 3, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
		"PEXPIRE":     {name: "PEXPIRE", minArity: 3, maxArity: 3, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
		"PEXPIREAT":   {name: "PEXPIREAT", minArity: 3, maxArity: 3, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
		"PERSIST":     {name: "PERSIST", minArity: 2, maxArity: 2, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
		"DEL":         {name: "DEL", minArity: 2, maxArity: variableArity, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: -1, keyStep: 1},
	}

	if len(commandRegistry) != len(expected) {
		t.Fatalf("expected %d registered commands, got %d", len(expected), len(commandRegistry))
	}

	for name, want := range expected {
		got, ok := commandRegistry[name]
		if !ok {
			t.Fatalf("missing command metadata for %s", name)
		}
		if got != want {
			t.Fatalf("metadata for %s mismatch: got %+v want %+v", name, got, want)
		}
	}
}

func TestCommandSpecForArgsNormalizesAndValidates(t *testing.T) {
	spec, err := commandSpecForArgs(commandTestArgs("get", "k"))
	if err != nil {
		t.Fatalf("lookup get failed: %v", err)
	}
	if spec.name != "GET" || spec.write || !spec.keyspace {
		t.Fatalf("unexpected GET spec: %+v", spec)
	}

	spec, err = commandSpecForArgs(commandTestArgs("set", "k", "v"))
	if err != nil {
		t.Fatalf("lookup set failed: %v", err)
	}
	if spec.name != "SET" || !spec.write || !spec.keyspace {
		t.Fatalf("unexpected SET spec: %+v", spec)
	}
}

func TestCommandSpecForArgsErrorsMatchExistingSemantics(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want string
	}{
		{name: "empty", args: nil, want: "empty command"},
		{name: "unknown", args: commandTestArgs("wat"), want: "unknown command 'WAT'"},
		{name: "too few", args: commandTestArgs("GET"), want: "wrong number of arguments for 'get'"},
		{name: "too many", args: commandTestArgs("PING", "a", "b"), want: "wrong number of arguments for 'ping'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := commandSpecForArgs(tt.args)
			if err == nil {
				t.Fatal("expected error")
			}
			if err.Error() != tt.want {
				t.Fatalf("got error %q, want %q", err.Error(), tt.want)
			}
		})
	}
}

func TestCommandSpecKeyArgumentIndexes(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want []int
	}{
		{name: "get", args: commandTestArgs("GET", "k"), want: []int{1}},
		{name: "mset", args: commandTestArgs("MSET", "a", "1", "b", "2"), want: []int{1, 3}},
		{name: "del", args: commandTestArgs("DEL", "a", "b", "c"), want: []int{1, 2, 3}},
		{name: "ping", args: commandTestArgs("PING"), want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := commandSpecForArgs(tt.args)
			if err != nil {
				t.Fatalf("commandSpecForArgs failed: %v", err)
			}
			got := spec.keyArgumentIndexes(len(tt.args))
			if len(got) != len(tt.want) {
				t.Fatalf("keyArgumentIndexes len=%d, want %d (%v)", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("keyArgumentIndexes[%d]=%d, want %d", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestMSETKeepsPairArityValidation(t *testing.T) {
	chdirTemp(t)

	db := MakeDbs()
	defer db.Close()

	_, err := db.Exec(0, commandTestArgs("MSET", "a", "1", "b"))
	if err == nil {
		t.Fatal("expected MSET with unmatched key/value pair to fail")
	}
	if err.Error() != "wrong number of arguments for 'mset'" {
		t.Fatalf("got error %q", err.Error())
	}
}

func commandTestArgs(args ...string) [][]byte {
	out := make([][]byte, 0, len(args))
	for _, arg := range args {
		out = append(out, []byte(arg))
	}
	return out
}
