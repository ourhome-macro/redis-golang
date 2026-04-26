package tcp

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestListenAndServeRejectsConnectionsOverMax(t *testing.T) {
	atomic.StoreInt32(&ClientCounter, 0)
	t.Cleanup(func() {
		atomic.StoreInt32(&ClientCounter, 0)
	})

	listener := newChannelListener()
	handler := newBlockingHandler()
	closeCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		listenAndServe(listener, handler, closeCh, &Config{MaxConnect: 1})
	}()

	server1, client1 := net.Pipe()
	defer client1.Close()
	sendConn(t, listener, server1)
	waitForHandler(t, handler)

	server2, client2 := net.Pipe()
	defer client2.Close()
	sendConn(t, listener, server2)

	_ = client2.SetReadDeadline(time.Now().Add(time.Second))
	_, err := client2.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected rejected connection to close")
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatal("timed out waiting for rejected connection to close")
	}

	if got := atomic.LoadInt32(&handler.handled); got != 1 {
		t.Fatalf("expected only one handled connection, got %d", got)
	}

	close(closeCh)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("server did not stop after close signal")
	}
}

func TestReadTimeoutConnAppliesIdleDeadline(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	conn := withReadTimeout(server, 20*time.Millisecond)
	errCh := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 1))
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected read timeout")
		}
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("expected timeout error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read did not time out")
	}
}

type channelListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newChannelListener() *channelListener {
	return &channelListener{
		conns:  make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

func (l *channelListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		if conn == nil {
			return nil, net.ErrClosed
		}
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *channelListener) Close() error {
	l.once.Do(func() {
		close(l.closed)
	})
	return nil
}

func (l *channelListener) Addr() net.Addr {
	return testAddr("listener")
}

type testAddr string

func (a testAddr) Network() string {
	return string(a)
}

func (a testAddr) String() string {
	return string(a)
}

type blockingHandler struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	handled int32
}

func newBlockingHandler() *blockingHandler {
	return &blockingHandler{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (h *blockingHandler) Handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	atomic.AddInt32(&h.handled, 1)
	h.entered <- struct{}{}

	select {
	case <-ctx.Done():
	case <-h.release:
	}
}

func (h *blockingHandler) Close() error {
	h.once.Do(func() {
		close(h.release)
	})
	return nil
}

func sendConn(t *testing.T, listener *channelListener, conn net.Conn) {
	t.Helper()

	select {
	case listener.conns <- conn:
	case <-time.After(time.Second):
		t.Fatal("server did not accept test connection")
	}
}

func waitForHandler(t *testing.T, handler *blockingHandler) {
	t.Helper()

	select {
	case <-handler.entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
}
