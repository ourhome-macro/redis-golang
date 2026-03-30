package main

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/redis/database"
	"MiddlewareSelf/tcp"
	"log"
	"time"
)

func main() {
	// 1. 准备配置
	// 这里配置监听本地的 8080 端口，设置最大连接数和超时时间
	cfg := &tcp.Config{
		Address:    ":8080",
		MaxConnect: 1000,
		Timeout:    10 * time.Second,
	}

	// 2. 准备 DB + AOF + Redis Handler
	db := database.MakeDbs()
	if err := db.EnableAOF(aof.SyncEverySec); err != nil {
		log.Fatalf("Enable AOF failed: %v", err)
	}
	if err := db.StartAutoRewriteLoop(2*time.Second, 1<<20, 100); err != nil {
		log.Fatalf("Start auto rewrite loop failed: %v", err)
	}
	handler := tcp.MakeRedisHandler(db)

	// 3. 启动服务
	// 这个函数会阻塞在这里，直到收到退出信号（比如 Ctrl+C）或者发生严重错误
	log.Println("Server is preparing to start...")
	err := tcp.ListenAndServeWithSignal(cfg, handler)
	if err != nil {
		log.Fatalf("Server start failed: %v", err)
	}

	log.Println("Server exited gracefully")
}
