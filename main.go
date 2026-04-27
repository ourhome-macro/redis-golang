package main

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/redis/database"
	"MiddlewareSelf/tcp"
	"flag"
	"log"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "redis server address")
	maxConn := flag.Uint("maxconn", 1000, "maximum concurrent client connections")
	timeout := flag.Duration("timeout", 10*time.Second, "idle read timeout")
	flag.Parse()

	// 1. 准备配置
	// 这里配置监听本地的 8080 端口，设置最大连接数和超时时间
	cfg := &tcp.Config{
		Address:    *addr,
		MaxConnect: uint32(*maxConn),
		Timeout:    *timeout,
	}

	// 2. 准备 DB + AOF + Redis Handler
	db, err := database.OpenDbs()
	if err != nil {
		log.Fatalf("Open DB failed: %v", err)
	}
	if err := db.EnableAOF(aof.SyncEverySec); err != nil {
		log.Fatalf("Enable AOF failed: %v", err)
	}
	if err := db.StartAutoRewriteLoop(2*time.Second, 1<<20, 100); err != nil {
		log.Fatalf("Start auto rewrite loop failed: %v", err)
	}
	if err := db.StartActiveExpireLoop(100*time.Millisecond, 1000); err != nil {
		log.Fatalf("Start active expire loop failed: %v", err)
	}
	handler := tcp.MakeRedisHandler(db)

	// 3. 启动服务
	// 这个函数会阻塞在这里，直到收到退出信号（比如 Ctrl+C）或者发生严重错误
	log.Println("Server is preparing to start...")
	err = tcp.ListenAndServeWithSignal(cfg, handler)
	if err != nil {
		log.Fatalf("Server start failed: %v", err)
	}

	log.Println("Server exited gracefully")
}
