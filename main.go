package main

import (
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

	// 2. 准备 Handler
	// 因为 EchoHandler 已经实现了 Handle 和 Close 方法，所以它实现了 Handler 接口
	handler := tcp.MakeEchoHandler()

	// 3. 启动服务
	// 这个函数会阻塞在这里，直到收到退出信号（比如 Ctrl+C）或者发生严重错误
	log.Println("Server is preparing to start...")
	err := tcp.ListenAndServeWithSignal(cfg, handler)
	if err != nil {
		log.Fatalf("Server start failed: %v", err)
	}

	log.Println("Server exited gracefully")
}
