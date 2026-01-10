package tcp

import (
	"MiddlewareSelf/util/atomic"
	"MiddlewareSelf/util/wait"
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type EchoClient struct {
	// tcp 连接
	Conn net.Conn
	// 当服务端开始发送数据时进入waiting, 阻止其它goroutine关闭连接
	// 带有最大等待时间的封装:
	Waiting wait.Wait
}

type EchoHandler struct {
	// 保存所有工作状态client的集合(把map当set用)
	// 并发安全的容器
	activeConn sync.Map
	// 关闭状态标识位
	closing atomic.Boolean
}

func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

func Close(c *EchoClient) error {
	c.Waiting.WaitWithTimeout(30 * time.Second)
	_ = c.Conn.Close()
	return nil
}

//func ListenAndServe1(address string) {
//	//绑定监听地址
//	listen, err := net.Listen("tcp", address)
//	if err != nil {
//		log.Fatal(fmt.Sprintf("listen err: %v", err))
//	}
//	defer listen.Close()
//	log.Printf("Listening on %s\n", address)
//	//accept
//	for {
//		conn, err := listen.Accept()
//		if err != nil {
//			log.Fatal(fmt.Sprintf("accept err: %v", err))
//		}
//		//处理该连接
//		go Handle(conn)
//	}
//}

func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
		return
	}
	client := &EchoClient{
		Conn: conn,
	}
	h.activeConn.Store(client, struct{}{})
	//read后write
	reader := bufio.NewReader(conn)
	defer func() {
		// 函数退出时，从集合中移除，并关闭连接
		h.activeConn.Delete(client)
		if err := Close(client); err != nil {
			log.Println("error closing client:", err)
		}
	}()
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("Client disconnected")
				//h.activeConn.Delete(client)
			} else {
				log.Printf("read error: %v", err)
			}
			return
		}
		log.Printf("Message from client: %s", msg)
		client.Waiting.Add(1)
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Write([]byte(msg))
		//conn.Write([]byte(msg))
		if err != nil {
			log.Printf("write error: %v", err)
			//return
		}
		client.Waiting.Done()
	}
}
