package main

import (
	"MiddlewareSelf/redis/config"
	"MiddlewareSelf/redis/database"
	"MiddlewareSelf/tcp"
	"flag"
	"log"
	"os"
)

func main() {
	cfg, err := config.ParseFlags(os.Args[0], os.Args[1:], os.Stderr)
	if err != nil {
		if err == flag.ErrHelp {
			return
		}
		log.Fatalf("Parse config failed: %v", err)
	}

	tcpCfg := cfg.TCPConfig()

	db, err := database.OpenDbs()
	if err != nil {
		log.Fatalf("Open DB failed: %v", err)
	}
	if err := db.EnableAOF(cfg.AOF.SyncPolicy); err != nil {
		log.Fatalf("Enable AOF failed: %v", err)
	}
	if cfg.AOF.AutoRewrite {
		if err := db.StartAutoRewriteLoop(
			cfg.AOF.AutoRewriteInterval,
			cfg.AOF.AutoRewriteMinSizeBytes,
			cfg.AOF.AutoRewriteGrowthPercent,
		); err != nil {
			log.Fatalf("Start auto rewrite loop failed: %v", err)
		}
	}
	if cfg.ActiveExpire.Enabled {
		if err := db.StartActiveExpireLoop(cfg.ActiveExpire.Interval, cfg.ActiveExpire.LimitPerDB); err != nil {
			log.Fatalf("Start active expire loop failed: %v", err)
		}
	}

	handler := tcp.MakeRedisHandler(db)
	log.Printf("Server is preparing to start on %s", tcpCfg.Address)
	err = tcp.ListenAndServeWithSignal(tcpCfg, handler)
	if err != nil {
		log.Fatalf("Server start failed: %v", err)
	}

	log.Println("Server exited gracefully")
}
