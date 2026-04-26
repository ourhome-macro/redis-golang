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
	listenAndServe(listener, handler, closeChan, nil)
}

func listenAndServe(listener net.Listener, handler Handler, closeChan <-chan struct{}, cfg *Config) {
	go func() {
		<-closeChan
		log.Println("close listener")
		_ = listener.Close()
		_ = handler.Close()
	}()

	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	var wait sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		if !acquireConnection(maxConnections(cfg)) {
			log.Printf("reject connection from %s: max connections reached", conn.RemoteAddr())
			_ = conn.Close()
			continue
		}

		conn = withReadTimeout(conn, readTimeout(cfg))
		wait.Add(1)
		go func(conn net.Conn) {
			defer atomic.AddInt32(&ClientCounter, -1)
			defer wait.Done()
			handler.Handle(context.Background(), conn)
		}(conn)
	}
	wait.Wait()
}

func maxConnections(cfg *Config) uint32 {
	if cfg == nil {
		return 0
	}
	return cfg.MaxConnect
}

func readTimeout(cfg *Config) time.Duration {
	if cfg == nil {
		return 0
	}
	return cfg.Timeout
}

func acquireConnection(max uint32) bool {
	if max == 0 {
		atomic.AddInt32(&ClientCounter, 1)
		return true
	}

	for {
		current := atomic.LoadInt32(&ClientCounter)
		if uint32(current) >= max {
			return false
		}
		if atomic.CompareAndSwapInt32(&ClientCounter, current, current+1) {
			return true
		}
	}
}

type readTimeoutConn struct {
	net.Conn
	timeout time.Duration
}

func withReadTimeout(conn net.Conn, timeout time.Duration) net.Conn {
	if timeout <= 0 {
		return conn
	}
	return &readTimeoutConn{
		Conn:    conn,
		timeout: timeout,
	}
}

func (c *readTimeoutConn) Read(b []byte) (int, error) {
	_ = c.Conn.SetReadDeadline(time.Now().Add(c.timeout))
	return c.Conn.Read(b)
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
	listenAndServe(listener, handler, closeChan, cfg)
	return nil
}
