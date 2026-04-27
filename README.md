# redis-golang

See [ROADMAP.md](./ROADMAP.md) for the execution order from single-node hardening to replication, sharding, and cluster work.

一个使用 Go 实现的 Redis 学习型项目，目标是把 **网络层 / RESP 协议 / 数据结构 / AOF 持久化 / Rewrite** 串成一个可运行骨架。

---

## ✨ 当前能力

- TCP Server 主流程（连接管理、优雅关闭）
- RESP 协议编解码（`+ - : $ *`）
- 基础命令执行：`SET` / `GET` / `DEL` / `SELECT`
- 过期命令：`SETWITHTTL` / `SETWITHPXAT` / `EXPIRE` / `PEXPIRE` / `PEXPIREAT` / `TTL` / `PTTL` / `PERSIST`
- 主动过期清理：后台周期性清理过期 key，并在 `INFO stats` 暴露计数
- 信息命令：`INFO` / `INFO persistence` / `INFO stats` / `INFO all`
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

Server flags:

```powershell
go run . --host 127.0.0.1 --port 8080 --maxconn 1000 --timeout 10s
```

Configuration defaults remain compatible with the previous hard-coded startup:

| Flag | Default | Description |
| --- | --- | --- |
| `--addr` | empty | Full `host:port` listen address override. When set, `--host` and `--port` are ignored. |
| `--host` | `127.0.0.1` | Listen host used when `--addr` is empty. |
| `--port` | `8080` | Listen port used when `--addr` is empty. |
| `--maxconn` | `1000` | Maximum concurrent client connections. `0` means unlimited. |
| `--timeout` | `10s` | Idle read timeout. `0` disables read deadlines. |
| `--aof-sync` | `everysec` | AOF fsync policy: `always`, `everysec`, or `no`. |
| `--aof-auto-rewrite` | `true` | Enable automatic AOF rewrite. |
| `--aof-auto-rewrite-interval` | `2s` | Automatic AOF rewrite check interval. |
| `--aof-auto-rewrite-min-size` | `1048576` | Minimum AOF size in bytes before automatic rewrite can trigger. |
| `--aof-auto-rewrite-growth-percent` | `100` | AOF growth percentage required to trigger automatic rewrite. |
| `--active-expire` | `true` | Enable active expiration loop. |
| `--active-expire-interval` | `100ms` | Active expiration loop interval. |
| `--active-expire-limit-per-db` | `1000` | Maximum expired keys removed per DB per cycle. `0` means unlimited. |

Validation rules:

- `--addr` takes precedence over `--host` and `--port`. Both `--addr` and `--host` are trimmed before validation.
- `--host` must be non-empty when `--addr` is not set. `--port` must stay within `1..65535`.
- `--maxconn` must stay within `0..4294967295`. `0` still means unlimited.
- `--timeout` must be `>= 0`. `0` still disables read deadlines.
- When `--aof-auto-rewrite=true`, `--aof-auto-rewrite-interval` must be `> 0`. If auto rewrite is disabled, the interval is still parsed but not used. `--aof-auto-rewrite-min-size` and `--aof-auto-rewrite-growth-percent` must both be `>= 0`.
- When `--active-expire=true`, `--active-expire-interval` must be `> 0`. If active expiration is disabled, the interval is still parsed but not used. `--active-expire-limit-per-db` must be `>= 0`, and `0` still means unlimited.

Default listen address remains `127.0.0.1:8080` (see `redis/config/config.go`).

默认监听地址：`127.0.0.1:8080`（见 `redis/config/config.go`）。

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
SET name redis-golang
GET name
EXPIRE name 30
TTL name
PERSIST name
DEL name
SELECT 1
INFO persistence
INFO stats
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
- 过期命令语义与 AOF 重放/重写保留过期时间
- 主动过期清理与 `INFO stats` 指标
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

A: 停掉占用进程，或通过 `--port` / `--addr` 指定其他监听地址。

---

## 📌 说明

这是一个学习与演进中的项目，优先清晰性与可实验性。欢迎继续扩展：事务、过期淘汰策略、更多数据结构与命令集。
