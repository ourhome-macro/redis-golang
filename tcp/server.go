package tcp

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Config 做tcp服务器配置
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

var ClientCounter int32

type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}

func ListenAndServe(listener net.Listener, handler Handler, closeChan <-chan struct{}) {
	// 监听关闭通知
	go func() {
		<-closeChan
		log.Println("close listener")
		_ = listener.Close()
		_ = handler.Close()
	}()

	defer func() {
		//atomic.AddInt32(&ClientCounter, -1)
		_ = listener.Close()
		_ = handler.Close()
	}()

	var wait sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		wait.Add(1)
		go func() {

			atomic.AddInt32(&ClientCounter, 1)
			defer atomic.AddInt32(&ClientCounter, -1)
			defer wait.Done()
			handler.Handle(context.Background(), conn)
		}()

	}
	wait.Wait()
}

func ListenAndServeWithSignal(cfg *Config, handler Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Println("receive signal", sig)
		switch sig {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	log.Println(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan)
	return nil
}
