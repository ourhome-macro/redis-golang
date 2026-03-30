package main

import (
	"MiddlewareSelf/redis/client"
	"context"
	"fmt"
	"log"
	"time"
)

func main() {
	cli, err := client.DialPipeline("127.0.0.1:8080", 3*time.Second)
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer cli.Close()

	cmds := []client.Command{
		client.NewCommand("SET", "name", "redis-golang"),
		client.NewCommand("GET", "name"),
		client.NewCommand("DEL", "name"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := cli.ExecStream(ctx, cmds)
	if err != nil {
		log.Fatalf("exec failed: %v", err)
	}

	for res := range ch {
		if res.Err != nil {
			log.Printf("response #%d failed, success-before=%d, err=%v", res.ResponseIndex, res.SuccessBefore, res.Err)
			continue
		}
		fmt.Printf("response #%d => %q\n", res.ResponseIndex, string(res.Reply.ToBytes()))
	}
}
