package config

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/tcp"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultHost    = "127.0.0.1"
	DefaultPort    = 8080
	DefaultMaxConn = 1000
	DefaultTimeout = 10 * time.Second

	DefaultAOFAutoRewriteInterval      = 2 * time.Second
	DefaultAOFAutoRewriteMinSizeBytes  = int64(1 << 20)
	DefaultAOFAutoRewriteGrowthPercent = 100.0

	DefaultActiveExpireInterval   = 100 * time.Millisecond
	DefaultActiveExpireLimitPerDB = 1000

	maxTCPConnections = uint64(^uint32(0))
)

type Config struct {
	Server       ServerConfig
	AOF          AOFConfig
	ActiveExpire ActiveExpireConfig
}

type ServerConfig struct {
	Host    string
	Port    int
	Address string
	MaxConn uint
	Timeout time.Duration
}

type AOFConfig struct {
	SyncPolicy               aof.SyncPolicy
	AutoRewrite              bool
	AutoRewriteInterval      time.Duration
	AutoRewriteMinSizeBytes  int64
	AutoRewriteGrowthPercent float64
}

type ActiveExpireConfig struct {
	Enabled    bool
	Interval   time.Duration
	LimitPerDB int
}

func Default() Config {
	return Config{
		Server: ServerConfig{
			Host:    DefaultHost,
			Port:    DefaultPort,
			MaxConn: DefaultMaxConn,
			Timeout: DefaultTimeout,
		},
		AOF: AOFConfig{
			SyncPolicy:               aof.SyncEverySec,
			AutoRewrite:              true,
			AutoRewriteInterval:      DefaultAOFAutoRewriteInterval,
			AutoRewriteMinSizeBytes:  DefaultAOFAutoRewriteMinSizeBytes,
			AutoRewriteGrowthPercent: DefaultAOFAutoRewriteGrowthPercent,
		},
		ActiveExpire: ActiveExpireConfig{
			Enabled:    true,
			Interval:   DefaultActiveExpireInterval,
			LimitPerDB: DefaultActiveExpireLimitPerDB,
		},
	}
}

func Parse(args []string) (Config, error) {
	return ParseFlags("redis-golang", args, io.Discard)
}

func ParseFlags(name string, args []string, output io.Writer) (Config, error) {
	cfg := Default()
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	if output == nil {
		output = io.Discard
	}
	fs.SetOutput(output)
	BindFlags(fs, &cfg)

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("unexpected positional arguments: %s", quoteArgs(fs.Args()))
	}
	cfg = normalizeConfig(cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func BindFlags(fs *flag.FlagSet, cfg *Config) {
	fs.StringVar(&cfg.Server.Address, "addr", cfg.Server.Address, "full listen address override in host:port form; when set, host and port flags are ignored")
	fs.StringVar(&cfg.Server.Host, "host", cfg.Server.Host, "listen host used when -addr is empty")
	fs.IntVar(&cfg.Server.Port, "port", cfg.Server.Port, "listen port used when -addr is empty; must be between 1 and 65535")
	fs.UintVar(&cfg.Server.MaxConn, "maxconn", cfg.Server.MaxConn, "maximum concurrent client connections; 0 means unlimited; must not exceed 4294967295")
	fs.DurationVar(&cfg.Server.Timeout, "timeout", cfg.Server.Timeout, "idle read timeout; 0 disables read deadlines; must not be negative")

	fs.Var(syncPolicyValue{target: &cfg.AOF.SyncPolicy}, "aof-sync", "AOF fsync policy: always, everysec, no")
	fs.BoolVar(&cfg.AOF.AutoRewrite, "aof-auto-rewrite", cfg.AOF.AutoRewrite, "enable automatic AOF rewrite")
	fs.DurationVar(&cfg.AOF.AutoRewriteInterval, "aof-auto-rewrite-interval", cfg.AOF.AutoRewriteInterval, "automatic AOF rewrite check interval; must be positive when -aof-auto-rewrite is true")
	fs.Int64Var(&cfg.AOF.AutoRewriteMinSizeBytes, "aof-auto-rewrite-min-size", cfg.AOF.AutoRewriteMinSizeBytes, "minimum AOF size in bytes before automatic rewrite can trigger; must not be negative")
	fs.Float64Var(&cfg.AOF.AutoRewriteGrowthPercent, "aof-auto-rewrite-growth-percent", cfg.AOF.AutoRewriteGrowthPercent, "AOF growth percentage required to trigger automatic rewrite; must not be negative")

	fs.BoolVar(&cfg.ActiveExpire.Enabled, "active-expire", cfg.ActiveExpire.Enabled, "enable active expiration loop")
	fs.DurationVar(&cfg.ActiveExpire.Interval, "active-expire-interval", cfg.ActiveExpire.Interval, "active expiration loop interval; must be positive when -active-expire is true")
	fs.IntVar(&cfg.ActiveExpire.LimitPerDB, "active-expire-limit-per-db", cfg.ActiveExpire.LimitPerDB, "maximum expired keys removed per DB per active expiration cycle; 0 means unlimited; must not be negative")
}

func (cfg Config) Validate() error {
	cfg = normalizeConfig(cfg)
	if cfg.Server.Address != "" {
		if err := validateAddress(cfg.Server.Address); err != nil {
			return fmt.Errorf("addr: %w", err)
		}
	} else {
		if cfg.Server.Host == "" {
			return fmt.Errorf("host must not be empty when addr is not set")
		}
		if err := validatePort(cfg.Server.Port); err != nil {
			return fmt.Errorf("port: %w", err)
		}
	}
	if uint64(cfg.Server.MaxConn) > maxTCPConnections {
		return fmt.Errorf("maxconn must be between 0 and %d", maxTCPConnections)
	}
	if cfg.Server.Timeout < 0 {
		return fmt.Errorf("timeout must not be negative")
	}
	if !isSyncPolicyValid(cfg.AOF.SyncPolicy) {
		return fmt.Errorf("aof-sync has invalid internal value %d", cfg.AOF.SyncPolicy)
	}
	if cfg.AOF.AutoRewriteInterval < 0 {
		return fmt.Errorf("aof-auto-rewrite-interval must not be negative")
	}
	if cfg.AOF.AutoRewrite && cfg.AOF.AutoRewriteInterval == 0 {
		return fmt.Errorf("aof-auto-rewrite-interval must be positive when aof-auto-rewrite is enabled")
	}
	if cfg.AOF.AutoRewriteMinSizeBytes < 0 {
		return fmt.Errorf("aof-auto-rewrite-min-size must not be negative")
	}
	if cfg.AOF.AutoRewriteGrowthPercent < 0 {
		return fmt.Errorf("aof-auto-rewrite-growth-percent must not be negative")
	}
	if cfg.ActiveExpire.Interval < 0 {
		return fmt.Errorf("active-expire-interval must not be negative")
	}
	if cfg.ActiveExpire.Enabled && cfg.ActiveExpire.Interval == 0 {
		return fmt.Errorf("active-expire-interval must be positive when active-expire is enabled")
	}
	if cfg.ActiveExpire.LimitPerDB < 0 {
		return fmt.Errorf("active-expire-limit-per-db must not be negative")
	}
	return nil
}

func (cfg Config) ListenAddress() string {
	cfg = normalizeConfig(cfg)
	if cfg.Server.Address != "" {
		return cfg.Server.Address
	}
	return net.JoinHostPort(cfg.Server.Host, strconv.Itoa(cfg.Server.Port))
}

func (cfg Config) TCPConfig() *tcp.Config {
	return &tcp.Config{
		Address:    cfg.ListenAddress(),
		MaxConnect: uint32(cfg.Server.MaxConn),
		Timeout:    cfg.Server.Timeout,
	}
}

func ParseSyncPolicy(value string) (aof.SyncPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "always":
		return aof.SyncAlways, nil
	case "everysec", "every-second", "every_second":
		return aof.SyncEverySec, nil
	case "no", "none":
		return aof.SyncNo, nil
	default:
		return 0, fmt.Errorf("unsupported AOF sync policy %q; want always, everysec, or no", value)
	}
}

func SyncPolicyName(policy aof.SyncPolicy) string {
	switch policy {
	case aof.SyncAlways:
		return "always"
	case aof.SyncEverySec:
		return "everysec"
	case aof.SyncNo:
		return "no"
	default:
		return fmt.Sprintf("unknown(%d)", policy)
	}
}

type syncPolicyValue struct {
	target *aof.SyncPolicy
}

func (v syncPolicyValue) String() string {
	if v.target == nil {
		return ""
	}
	return SyncPolicyName(*v.target)
}

func (v syncPolicyValue) Set(raw string) error {
	if v.target == nil {
		return fmt.Errorf("nil sync policy target")
	}
	policy, err := ParseSyncPolicy(raw)
	if err != nil {
		return err
	}
	*v.target = policy
	return nil
}

func isSyncPolicyValid(policy aof.SyncPolicy) bool {
	switch policy {
	case aof.SyncAlways, aof.SyncEverySec, aof.SyncNo:
		return true
	default:
		return false
	}
}

func normalizeConfig(cfg Config) Config {
	cfg.Server.Address = strings.TrimSpace(cfg.Server.Address)
	cfg.Server.Host = strings.TrimSpace(cfg.Server.Host)
	return cfg
}

func quoteArgs(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, strconv.Quote(arg))
	}
	return strings.Join(quoted, ", ")
}

func validateAddress(address string) error {
	_, portText, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("must be in host:port form: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return fmt.Errorf("port must be numeric")
	}
	return validatePort(port)
}

func validatePort(port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("must be between 1 and 65535")
	}
	return nil
}
