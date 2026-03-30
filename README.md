# redis-golang

一个使用 Go 实现的 Redis 学习型项目，目标是把 **网络层 / RESP 协议 / 数据结构 / AOF 持久化 / Rewrite** 串成一个可运行骨架。

---

## ✨ 当前能力

- TCP Server 主流程（连接管理、优雅关闭）
- RESP 协议编解码（`+ - : $ *`）
- 基础命令执行：`SET` / `GET` / `DEL` / `SELECT` / `SETWITHTTL`
- 跳表（含 span/rank）：支持插入、删除、按 rank 查询、TopN
- AOF 持久化：`appendonly.aof`
- AOF Rewrite（高仿 Redis 思路）：
	- 子协程快照写 `temp.aof`
	- 主线程重写缓冲区增量收集
	- 合并增量 + 原子替换
	- 失败回滚与清理
	- 自动重写触发（按文件大小增长阈值）
- 客户端：
	- 流式 Pipeline 客户端（逐条收发，错误定位到第 N 条）
	- 交互式 `redis-cli-lite`（上下键历史、Tab 补全、多行输入）

---

## 🧱 主要目录

- `main.go`：服务端启动入口
- `tcp/`：网络框架与 Redis handler
- `redis/parser/`：RESP 解析
- `redis/resp/`：RESP 回复编码
- `redis/database/`：命令执行与 DB 逻辑
- `redis/aof/`：AOF 持久化与 rewrite
- `redis/client/`：Pipeline 客户端
- `cmd/redis-cli-lite/`：交互 CLI
- `cmd/pipeline-client/`：Pipeline 示例客户端

---

## 🚀 快速开始

### 1) 启动服务端

在项目根目录执行：

```powershell
go run .
```

默认监听地址：`127.0.0.1:8080`（见 `main.go`）。

### 2) 启动交互 CLI

新开一个终端执行：

```powershell
go run ./cmd/redis-cli-lite
```

指定地址/超时：

```powershell
go run ./cmd/redis-cli-lite --addr 127.0.0.1:8080 --timeout 3s
```

示例命令：

```text
PING
SET name redis-golang
GET name
DEL name
SELECT 1
```

### 3) 运行 Pipeline 示例客户端

```powershell
go run ./cmd/pipeline-client
```

---

## 🧪 测试

```powershell
go test ./...
```

重点测试覆盖：

- RESP 解析边界
- 跳表 rank/span 逻辑
- Pipeline 流式收发与第 N 条失败定位
- AOF Rewrite 增量合并、回滚恢复、自动触发

---

## 📝 AOF Rewrite 设计说明（简版）

重写流程：

1. 标记 `rewriting=true`，主线程继续处理写请求，同时把写命令追加到 `rewriteBuffer`。
2. 子协程读取快照并写 `temp.aof`。
3. 子协程结束后，主线程把 `rewriteBuffer` 合并到 `temp.aof` 末尾。
4. `os.Rename` 原子替换 `appendonly.aof`。
5. 重开 AOF 文件句柄，继续服务。

> Redis C 版依赖 `fork + COW`；本项目在 Go 中用“快照复制 + 锁 + 重写缓冲”模拟该语义。

---

## ❗常见问题

### Q1: `go main.go` 报错？

A: 这是错误命令，请使用：

```powershell
go run .
```

### Q2: `go run pipeline.go` 报错？

A: `pipeline.go` 是库代码不是 `main` 入口。请使用：

```powershell
go run ./cmd/pipeline-client
```

### Q3: 服务端启动报 8080 端口被占用？

A: 停掉占用进程，或修改 `main.go` 中监听端口。

---

## 📌 说明

这是一个学习与演进中的项目，优先清晰性与可实验性。欢迎继续扩展：事务、过期淘汰策略、更多数据结构与命令集。
