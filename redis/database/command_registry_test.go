package database

import "testing"

func TestCommandRegistryMetadata(t *testing.T) {
	expected := map[string]commandSpec{
		"PING":         {name: "PING", minArity: 1, maxArity: 2},
		"ECHO":         {name: "ECHO", minArity: 2, maxArity: 2},
		"INFO":         {name: "INFO", minArity: 1, maxArity: 2},
		"SELECT":       {name: "SELECT", minArity: 2, maxArity: 2},
		"BGREWRITEAOF": {name: "BGREWRITEAOF", minArity: 1, maxArity: 1},

		"EXISTS": {name: "EXISTS", minArity: 2, maxArity: variableArity, keyspace: true},
		"MGET":   {name: "MGET", minArity: 2, maxArity: variableArity, keyspace: true},
		"GET":    {name: "GET", minArity: 2, maxArity: 2, keyspace: true},
		"TTL":    {name: "TTL", minArity: 2, maxArity: 2, keyspace: true},
		"PTTL":   {name: "PTTL", minArity: 2, maxArity: 2, keyspace: true},

		"SET":         {name: "SET", minArity: 3, maxArity: 3, write: true, keyspace: true},
		"MSET":        {name: "MSET", minArity: 3, maxArity: variableArity, write: true, keyspace: true},
		"SETWITHTTL":  {name: "SETWITHTTL", minArity: 4, maxArity: 4, write: true, keyspace: true},
		"SETWITHPXAT": {name: "SETWITHPXAT", minArity: 4, maxArity: 4, write: true, keyspace: true},
		"EXPIRE":      {name: "EXPIRE", minArity: 3, maxArity: 3, write: true, keyspace: true},
		"PEXPIRE":     {name: "PEXPIRE", minArity: 3, maxArity: 3, write: true, keyspace: true},
		"PEXPIREAT":   {name: "PEXPIREAT", minArity: 3, maxArity: 3, write: true, keyspace: true},
		"PERSIST":     {name: "PERSIST", minArity: 2, maxArity: 2, write: true, keyspace: true},
		"DEL":         {name: "DEL", minArity: 2, maxArity: variableArity, write: true, keyspace: true},
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
