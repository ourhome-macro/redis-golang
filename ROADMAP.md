# Project Roadmap

This file turns the follow-up work into an execution order instead of a vague idea list.

## Current Position

The project already has:

- TCP server and RESP parsing
- Basic string and DB commands
- Expiration command surface: `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`
- AOF persistence and rewrite
- Pipeline client and a lightweight CLI
- Basic `INFO` / `INFO persistence`

What it does not have yet is the operational and semantic stability needed before distributed work starts.

## Core Rule

Do not start cluster work until single-node behavior is predictable.

A buggy single-node Redis clone becomes a much harder-to-debug distributed system once replication, sharding, routing, and failover are layered on top.

## Phase 1: Harden Single Node

Target: make one node reliable enough that persistence, expiration, observability, and error semantics are boring.

Work items:

1. Expiration semantics
- Done: command support for `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`
- Remaining: add active expiration cycle instead of relying only on lazy deletion
- Remaining: harden restart/rewrite tests for expiration correctness

2. Persistence control
- Add `BGREWRITEAOF` command
- Expose more persistence state through `INFO`
- Decide how AOF write failure should affect future writes

3. Command and protocol coverage
- Planned: add `PING`, `ECHO`, `EXISTS`, `MGET`, `MSET`
- Normalize error replies to be closer to Redis
- Add more protocol edge-case tests

4. Operational visibility
- Expand `INFO` beyond `persistence`
- Add `INFO server`, `INFO stats`, and simple counters
- Add logging around slow operations and rewrite lifecycle

5. Test and recovery discipline
- Crash/restart recovery tests
- AOF corruption tolerance tests
- Basic benchmark scripts for parser, write path, and rewrite

Exit gate for Phase 1:

- Single-node restart behavior is stable
- Expiration is complete enough to trust in tests
- Persistence and background tasks are observable
- Command/error semantics stop changing every few commits

## Phase 2: Replication

Target: build master-replica before cluster.

Why this comes first:

- Cluster without replication is incomplete
- Replication forces the codebase to define authoritative write flow and state propagation
- Full sync and incremental sync expose design flaws early

Work items:

1. Replica handshake
- Add `REPLCONF`
- Add a simplified `PSYNC`
- Add role metadata in `INFO replication`

2. Full synchronization
- Master sends snapshot plus incremental tail
- Replica loads snapshot and catches up

3. Incremental replication
- Replication backlog
- Offsets and ACKs
- Reconnect and partial resync path

4. Safety and tests
- Master write path tests with attached replicas
- Replica reconnect tests
- Offset consistency tests

Exit gate for Phase 2:

- One master can reliably feed at least one replica
- Full sync and reconnect paths are stable
- Replication offsets and backlog logic are observable

## Phase 3: Proxy Sharding (Optional but Recommended)

Target: enter multi-node work with lower complexity than Redis Cluster.

This is the practical learning stage if the goal is to move into distributed behavior early without taking on Redis Cluster all at once.

Work items:

1. Build a proxy layer
- Route keys by consistent hashing
- Keep storage nodes simple
- Return a clear error for multi-key cross-shard commands

2. Observability
- Per-node routing counters
- Basic shard rebalance tooling

3. Failure model
- Define what happens when a shard is down
- Decide whether writes fail closed or route nowhere

Exit gate for Phase 3:

- Multi-node routing works reliably
- The team understands key distribution, resharding pain, and cross-shard command limits

## Phase 4: Redis-Cluster-Like Mode

Target: implement a real slot-based cluster model closer to Redis Cluster.

Only start this after replication is in place and the team has accepted the extra complexity.

Work items:

1. Slot model
- 16384 hash slots
- Per-node slot ownership
- `CLUSTER SLOTS` style metadata

2. Client redirection
- `MOVED`
- `ASK`
- Slot cache behavior assumptions

3. Master-replica topology
- Master owns slots
- Replica follows master
- Promote replica on failure in a simplified failover model

4. Cluster metadata propagation
- Node membership
- Slot map updates
- Health and state transitions

Exit gate for Phase 4:

- Clients can route by slots
- Failover updates slot ownership cleanly
- Cluster metadata converges under expected scenarios

## When To Do Cluster

Short answer: after Phase 2, not now.

If the goal is to learn distributed systems faster, start Phase 3 after Phase 2.
If the goal is Redis compatibility, skip straight from Phase 2 to Phase 4.

## Recommended Next Execution Item

Start with:

1. Active expiration cycle
2. Expiration restart/rewrite correctness hardening
3. `BGREWRITEAOF`

Reason:

- Expiration is a core Redis behavior and affects memory, persistence, and correctness
- The basic expire command surface exists; active cleanup and persistence edge cases are still required before multi-node work
- `BGREWRITEAOF` completes the current AOF feature line before replication starts

## Commit-Scale Breakdown

Use small commits with one theme each:

1. Add active expiration cycle and metrics
2. Add restart/rewrite coverage for expiration correctness
3. Add `BGREWRITEAOF` command and state reporting
4. Expand `INFO` sections
5. Add planned compatibility commands (`PING`, `ECHO`, `EXISTS`, `MGET`, `MSET`) or start replication handshake

That keeps the project reviewable and lowers rollback cost when semantics need to be corrected.
