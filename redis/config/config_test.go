package config

import (
	"MiddlewareSelf/redis/aof"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigMatchesCurrentStartupBehavior(t *testing.T) {
	cfg := Default()

	if got := cfg.ListenAddress(); got != "127.0.0.1:8080" {
		t.Fatalf("ListenAddress() = %q, want %q", got, "127.0.0.1:8080")
	}
	if cfg.Server.MaxConn != 1000 {
		t.Fatalf("MaxConn = %d, want 1000", cfg.Server.MaxConn)
	}
	if cfg.Server.Timeout != 10*time.Second {
		t.Fatalf("Timeout = %s, want 10s", cfg.Server.Timeout)
	}
	if cfg.AOF.SyncPolicy != aof.SyncEverySec {
		t.Fatalf("SyncPolicy = %v, want SyncEverySec", cfg.AOF.SyncPolicy)
	}
	if !cfg.AOF.AutoRewrite {
		t.Fatal("AutoRewrite = false, want true")
	}
	if cfg.AOF.AutoRewriteInterval != 2*time.Second {
		t.Fatalf("AutoRewriteInterval = %s, want 2s", cfg.AOF.AutoRewriteInterval)
	}
	if cfg.AOF.AutoRewriteMinSizeBytes != 1<<20 {
		t.Fatalf("AutoRewriteMinSizeBytes = %d, want %d", cfg.AOF.AutoRewriteMinSizeBytes, 1<<20)
	}
	if cfg.AOF.AutoRewriteGrowthPercent != 100 {
		t.Fatalf("AutoRewriteGrowthPercent = %f, want 100", cfg.AOF.AutoRewriteGrowthPercent)
	}
	if !cfg.ActiveExpire.Enabled {
		t.Fatal("ActiveExpire.Enabled = false, want true")
	}
	if cfg.ActiveExpire.Interval != 100*time.Millisecond {
		t.Fatalf("ActiveExpire.Interval = %s, want 100ms", cfg.ActiveExpire.Interval)
	}
	if cfg.ActiveExpire.LimitPerDB != 1000 {
		t.Fatalf("ActiveExpire.LimitPerDB = %d, want 1000", cfg.ActiveExpire.LimitPerDB)
	}

	tcpCfg := cfg.TCPConfig()
	if tcpCfg.Address != "127.0.0.1:8080" || tcpCfg.MaxConnect != 1000 || tcpCfg.Timeout != 10*time.Second {
		t.Fatalf("TCPConfig() = %+v, want compatible TCP defaults", tcpCfg)
	}
}

func TestParseFlagsOverridesDefaults(t *testing.T) {
	cfg, err := Parse([]string{
		"-host", "0.0.0.0",
		"-port", "6380",
		"-maxconn", "2048",
		"-timeout", "5s",
		"-aof-sync", "always",
		"-aof-auto-rewrite=false",
		"-aof-auto-rewrite-interval", "3s",
		"-aof-auto-rewrite-min-size", "2097152",
		"-aof-auto-rewrite-growth-percent", "50.5",
		"-active-expire=false",
		"-active-expire-interval", "250ms",
		"-active-expire-limit-per-db", "500",
	})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if got := cfg.ListenAddress(); got != "0.0.0.0:6380" {
		t.Fatalf("ListenAddress() = %q, want %q", got, "0.0.0.0:6380")
	}
	if cfg.Server.MaxConn != 2048 {
		t.Fatalf("MaxConn = %d, want 2048", cfg.Server.MaxConn)
	}
	if cfg.Server.Timeout != 5*time.Second {
		t.Fatalf("Timeout = %s, want 5s", cfg.Server.Timeout)
	}
	if cfg.AOF.SyncPolicy != aof.SyncAlways {
		t.Fatalf("SyncPolicy = %v, want SyncAlways", cfg.AOF.SyncPolicy)
	}
	if cfg.AOF.AutoRewrite {
		t.Fatal("AutoRewrite = true, want false")
	}
	if cfg.AOF.AutoRewriteInterval != 3*time.Second {
		t.Fatalf("AutoRewriteInterval = %s, want 3s", cfg.AOF.AutoRewriteInterval)
	}
	if cfg.AOF.AutoRewriteMinSizeBytes != 2097152 {
		t.Fatalf("AutoRewriteMinSizeBytes = %d, want 2097152", cfg.AOF.AutoRewriteMinSizeBytes)
	}
	if cfg.AOF.AutoRewriteGrowthPercent != 50.5 {
		t.Fatalf("AutoRewriteGrowthPercent = %f, want 50.5", cfg.AOF.AutoRewriteGrowthPercent)
	}
	if cfg.ActiveExpire.Enabled {
		t.Fatal("ActiveExpire.Enabled = true, want false")
	}
	if cfg.ActiveExpire.Interval != 250*time.Millisecond {
		t.Fatalf("ActiveExpire.Interval = %s, want 250ms", cfg.ActiveExpire.Interval)
	}
	if cfg.ActiveExpire.LimitPerDB != 500 {
		t.Fatalf("ActiveExpire.LimitPerDB = %d, want 500", cfg.ActiveExpire.LimitPerDB)
	}
}

func TestAddressFlagOverridesHostAndPort(t *testing.T) {
	cfg, err := Parse([]string{
		"-addr", "127.0.0.1:7000",
		"-host", "0.0.0.0",
		"-port", "6380",
	})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if got := cfg.ListenAddress(); got != "127.0.0.1:7000" {
		t.Fatalf("ListenAddress() = %q, want addr override", got)
	}
}

func TestParseNormalizesWhitespaceInAddressInputs(t *testing.T) {
	t.Run("host", func(t *testing.T) {
		cfg, err := Parse([]string{
			"-host", " 0.0.0.0 ",
			"-port", "6380",
		})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if got := cfg.Server.Host; got != "0.0.0.0" {
			t.Fatalf("Server.Host = %q, want %q", got, "0.0.0.0")
		}
		if got := cfg.ListenAddress(); got != "0.0.0.0:6380" {
			t.Fatalf("ListenAddress() = %q, want %q", got, "0.0.0.0:6380")
		}
	})

	t.Run("addr", func(t *testing.T) {
		cfg, err := Parse([]string{
			"-addr", " 127.0.0.1:7000 ",
			"-host", "0.0.0.0",
			"-port", "6380",
		})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if got := cfg.Server.Address; got != "127.0.0.1:7000" {
			t.Fatalf("Server.Address = %q, want %q", got, "127.0.0.1:7000")
		}
		if got := cfg.ListenAddress(); got != "127.0.0.1:7000" {
			t.Fatalf("ListenAddress() = %q, want %q", got, "127.0.0.1:7000")
		}
	})
}

func TestParseAllowsZeroIntervalsWhenLoopsDisabled(t *testing.T) {
	cfg, err := Parse([]string{
		"-aof-auto-rewrite=false",
		"-aof-auto-rewrite-interval=0",
		"-active-expire=false",
		"-active-expire-interval=0",
	})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if cfg.AOF.AutoRewrite {
		t.Fatal("AutoRewrite = true, want false")
	}
	if cfg.AOF.AutoRewriteInterval != 0 {
		t.Fatalf("AutoRewriteInterval = %s, want 0", cfg.AOF.AutoRewriteInterval)
	}
	if cfg.ActiveExpire.Enabled {
		t.Fatal("ActiveExpire.Enabled = true, want false")
	}
	if cfg.ActiveExpire.Interval != 0 {
		t.Fatalf("ActiveExpire.Interval = %s, want 0", cfg.ActiveExpire.Interval)
	}
}

func TestParseRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "invalid aof sync", args: []string{"-aof-sync", "sometimes"}, want: `unsupported AOF sync policy "sometimes"; want always, everysec, or no`},
		{name: "invalid port", args: []string{"-port", "70000"}, want: "port: must be between 1 and 65535"},
		{name: "invalid addr", args: []string{"-addr", "127.0.0.1"}, want: "addr: must be in host:port form"},
		{name: "negative timeout", args: []string{"-timeout=-1s"}, want: "timeout must not be negative"},
		{name: "zero rewrite interval", args: []string{"-aof-auto-rewrite-interval=0"}, want: "aof-auto-rewrite-interval must be positive when aof-auto-rewrite is enabled"},
		{name: "negative rewrite interval", args: []string{"-aof-auto-rewrite-interval=-1s"}, want: "aof-auto-rewrite-interval must not be negative"},
		{name: "negative rewrite min size", args: []string{"-aof-auto-rewrite-min-size=-1"}, want: "aof-auto-rewrite-min-size must not be negative"},
		{name: "negative rewrite growth", args: []string{"-aof-auto-rewrite-growth-percent=-1"}, want: "aof-auto-rewrite-growth-percent must not be negative"},
		{name: "zero active expire interval", args: []string{"-active-expire-interval=0"}, want: "active-expire-interval must be positive when active-expire is enabled"},
		{name: "negative active expire interval", args: []string{"-active-expire-interval=-1s"}, want: "active-expire-interval must not be negative"},
		{name: "negative active expire limit", args: []string{"-active-expire-limit-per-db=-1"}, want: "active-expire-limit-per-db must not be negative"},
		{name: "blank host", args: []string{"-host", "   "}, want: "host must not be empty when addr is not set"},
		{name: "unexpected positional", args: []string{"extra"}, want: `unexpected positional arguments: "extra"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.args)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Parse error = %q, want substring %q", err.Error(), tt.want)
			}
		})
	}
}

func TestParseSyncPolicyNames(t *testing.T) {
	tests := []struct {
		raw  string
		want aof.SyncPolicy
	}{
		{raw: "always", want: aof.SyncAlways},
		{raw: "everysec", want: aof.SyncEverySec},
		{raw: "every-second", want: aof.SyncEverySec},
		{raw: " Every_Second ", want: aof.SyncEverySec},
		{raw: "no", want: aof.SyncNo},
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := ParseSyncPolicy(tt.raw)
			if err != nil {
				t.Fatalf("ParseSyncPolicy failed: %v", err)
			}
			if got != tt.want {
				t.Fatalf("ParseSyncPolicy(%q) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestValidateRejectsInvalidInternalSyncPolicy(t *testing.T) {
	cfg := Default()
	cfg.AOF.SyncPolicy = aof.SyncPolicy(99)

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate succeeded, want error")
	}
	if !strings.Contains(err.Error(), "aof-sync has invalid internal value 99") {
		t.Fatalf("Validate error = %q, want invalid sync policy message", err.Error())
	}
}

func TestParseRejectsMaxConnBeyondTCPRange(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("uint flag parsing is too small on 32-bit platforms for this overflow case")
	}

	_, err := Parse([]string{"-maxconn", "4294967296"})
	if err == nil {
		t.Fatal("Parse succeeded, want error")
	}
	if !strings.Contains(err.Error(), "maxconn must be between 0 and 4294967295") {
		t.Fatalf("Parse error = %q, want maxconn range message", err.Error())
	}
}
