# Benchmark Baselines

Parser:
- `go test ./redis/parser -run ^$ -bench BenchmarkParseRESP -benchmem`

Write path:
- `go test ./redis/database -run ^$ -bench BenchmarkExecSet -benchmem`

Rewrite:
- `go test ./redis/aof -run ^$ -bench BenchmarkRewrite -benchmem`

Notes:
- The parser benchmark calls internal `parse` directly so the numbers focus on the parsing path, not goroutine startup in `ParseStream`.
- The write path benchmark measures steady-state `Db.Exec` for `SET bench:key <32B>` with AOF off, `SyncNo`, and `SyncAlways`.
- The rewrite benchmark measures repeated `AOF.Rewrite` runs over prebuilt snapshots of `SET` commands.
