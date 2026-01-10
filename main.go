package main

import "MiddlewareSelf/tcp"

func main() {
	tcp.ListenAndServe(":8080")
}
