package tcp

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
)

func ListenAndServe(address string) {
	//绑定监听地址
	listen, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(fmt.Sprintf("listen err: %v", err))
	}
	defer listen.Close()
	log.Printf("Listening on %s\n", address)
	//accept
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatal(fmt.Sprintf("accept err: %v", err))
		}
		//处理该连接
		go Handle(conn)
	}
}

func Handle(conn net.Conn) {
	//read后write
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("Client disconnected")
			} else {
				log.Printf("read error: %v", err)
			}
			return
		}
		log.Printf("Message from client: %s", msg)
		conn.Write([]byte(msg))
	}
}
