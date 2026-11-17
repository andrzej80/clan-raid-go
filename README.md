# raidboss

[Русская версия](README.ru.md)

A backend service for a clan raid boss, written in Go. Clans fight a shared boss, and thousands of players hit one HP pool at the same time. The boss dies exactly once, rewards are paid exactly once, and a damage leaderboard updates live.

![raidwatch: the boss dies under botsim load](docs/demo.gif)

The service is built from gRPC, Kafka, PostgreSQL and Redis. PostgreSQL is the source of truth, Kafka carries hits and events, Redis holds the leaderboard, an HP cache and rate limits. Everything is traced with OpenTelemetry. This README describes what is implemented and how it works. Limits of the current state are listed in [Status and limitations](#status-and-limitations).

## How it works

### The game rules

- A clan has at most one active raid. A partial unique index in Postgres enforces it.
- A player can hit only the boss of their own clan, and only while the raid is `active`.
- The server calculates damage: `round(power × u × crit)`, where `power` is 100–1000, `u` is uniform in [0.8, 1.2), and `crit` is 2 with 10% probability, otherwise 1.
- Rate limit: 5 hits per second per player, burst 10. Above that, the call fails with `RESOURCE_EXHAUSTED` and a `RetryInfo` delay.
- The hit that brings HP to 0 is the killing blow. Hits after it do not count.
- A raid that is not killed before `ends_at` becomes `expired` and pays no rewards.
- Rewards for a defeated boss: `participation` (`100 + floor(1000 × damage share)`) for everyone who hit it, `top3` (+500 / +300 / +200), and `killing_blow` (+250).
- A repeated request with the same `idempotency_key` does not create a second hit.

### Architecture

```
 botsim ──gRPC──► api ──produce──► Kafka: raid.hits (key = raid_id)
                   │                          │
                   │ rate limit, cache        ▼
                   ▼                  hits-consumer ──tx──► PostgreSQL
                 Redis ◄──────────────────────┘ ZADD/SET/PUBLISH      │
                   │                                                  │ outbox
                   │ pub/sub                                          ▼
                   ▼                                            outbox-relay
   api (WatchRaid, gRPC stream) ──► clients                           │
                                                                      ▼
                                        Kafka: raid.events, rewards.granted
                                                      │
                                                      ▼
                                               rewards-worker ──tx──► PostgreSQL
```

| Binary | What it does |
|---|---|
| `api` | gRPC API: accepts hits, serves the `WatchRaid` stream and the leaderboard. A separate HTTP port serves pprof only |
| `hits-consumer` | Reads `raid.hits`, applies damage in Postgres, updates Redis |
| `rewards-worker` | Reads `raid.events`, grants rewards |
| `outbox-relay` | Moves events from the `outbox` table to Kafka |
| `scheduler` | Marks overdue raids as `expired` (every 10 s by default) |
| `botsim` | Load simulator: players as goroutines, open and closed load models, p50/p95/p99 report |
| `raidctl` | CLI: `seed`, `verify`, `rebuild-cache`, `outbox dump` |
| `raidwatch` | Terminal client that shows a live raid through `WatchRaid` |

Kafka topics: `raid.hits` (key `raid_id`, 12 partitions), `raid.events` (key `raid_id`, 6), `rewards.granted` (key `player_id`, 6), and dead-letter topics `raid.hits.dlq` and `raid.events.dlq`. Messages are protobuf.

### The path of one hit

1. **`api` receives `Attack`.** Interceptors run in this order: OpenTelemetry stats handler, recovery, logging, auth (the player comes from the `x-player-id` metadata), rate limit. The rate limit is a token bucket in Redis, in one Lua script that reads time with `TIME` inside Redis.
2. **Validation.** The player and the raid are read from an in-process TTL cache. A cache miss goes through `singleflight`, so many concurrent requests for one raid cause one database read. The clan and the raid status are checked here as an optimisation, and the final decision belongs to the consumer.
3. **Publish.** `api` calculates damage and sends a `HitEvent` to Kafka with the raid id as the key. The reply to the client comes after the broker confirms the write. A successful reply means "accepted into the queue", not "counted". The result (counted or too late) comes through `WatchRaid`.
4. **`hits-consumer`** has one goroutine per assigned partition and collects batches of up to 500 hits or 50 ms. It groups the batch by raid, keeping order, drops duplicate `hit_id`s, and sends unparsable messages to the dead-letter topic with the reason in a header. For each raid it runs one transaction:
   - lock the raid row (`SELECT … FOR UPDATE`);
   - if the raid is not `active`, count all hits as late;
   - skip hits that are already stored (`hit_id` lookup);
   - apply hits in order; the first one that brings HP to 0 or below is the killing blow, and the rest of the batch is late;
   - insert accepted hits into `raid_hits` (they get an ordering `seq`), upsert `raid_damage` and read back absolute totals;
   - update `raids.hp_left`, and for a killed boss set `defeated`, `killer_*`, `finished_at` and write a `RaidDefeated` event to the `outbox` table in the same transaction.
5. **After commit,** the consumer updates Redis with absolute values (`ZADD` for the leaderboard, `SET` for HP) and publishes updates to a pub/sub channel. Kafka offsets are committed only after all raids of the batch are done.
6. **`outbox-relay`** reads unsent rows (`FOR UPDATE SKIP LOCKED`, batches of 100), produces them to Kafka synchronously, and marks them sent in the same transaction. Several relays can run in parallel. This keeps per-raid order where it matters but not global order.
7. **`rewards-worker`** consumes `RaidDefeated`, reads `raid_damage`, calculates rewards with a pure function, and inserts them with `ON CONFLICT DO NOTHING`. For rows that were really inserted, it writes `RewardGranted` to the outbox.

### Why the results stay correct

Kafka delivers at least once, and the code is written for that: offsets are committed only after the database write, and every step is safe to repeat.

- **Consumer crashed after the Postgres commit, before the offset commit.** The batch comes again. Already stored hits are skipped by `hit_id`, so nothing is applied twice.
- **Crash before the Redis update.** A retry restores the leaderboard, because `ZADD` carries absolute totals. If there is no retry, `raidctl rebuild-cache` rebuilds Redis from Postgres.
- **Rebalance with two consumers briefly handling one partition.** `FOR UPDATE` serialises them, and the primary key on `raid_hits.hit_id` blocks a double write.
- **Duplicate rewards.** A unique constraint on `(raid, player, kind)` rejects them.
- **Hot row.** All hits for one raid go to one partition and one consumer, so concurrent writers do not fight over the boss row. This is the main design decision, and the [experiment below](#the-hot-row-experiment) measures it.

### Live updates: `WatchRaid`

`WatchRaid` is a server-streaming RPC. The first message is a `snapshot`, so a client that reconnects gets the current state at once. After that come `hp_left`, `hits_total`, leaderboard updates and a final `finished` message.

- Each `api` instance holds one Redis pub/sub subscription per raid while that raid has local subscribers.
- The hub never calls `stream.Send`. Every subscriber has its own bounded buffer (16 messages by default), and the RPC handler goroutine reads from it and sends. A slow client cannot block the others.
- When a buffer is full, one update waits in a spare slot. If another arrives before the client reads, the client is disconnected with `RESOURCE_EXHAUSTED`. The `finished` message is never dropped.
- Updates are throttled to one per 250 ms per raid, and values that pile up are collapsed to the latest ones. All updates carry absolute values, so collapsing loses nothing.
- The hub state is guarded by `sync.RWMutex`. An actor version (one owner goroutine) was built and measured, and lost under load with many raids and reconnect storms; the comparison is in [docs/pr-stage-6.md](docs/pr-stage-6.md) (in Russian).

### Resilience

- **Kafka down.** The hit publisher has a bounded send time and a circuit breaker. While the breaker is open, `api` answers `UNAVAILABLE` at once instead of waiting. Recovery is decided by a health check (a ping to the broker), not by time; after that a small share of real hits is let through as probes, and the breaker closes after several successes and a quiet period. A client that retries with the same `idempotency_key` is safe.
- **Redis down.** The rate limiter falls back from the shared Redis limit to a per-instance in-memory limit, behind its own circuit breaker, so a dead Redis does not add a timeout to every request. The effective limit loosens to "N instances × the normal rate" but does not vanish.
- **Bad messages.** A message that cannot be parsed goes to a dead-letter topic with the reason, and processing continues.
- **PostgreSQL pool.** One `pgxpool` per process, configured from env. Pool statistics are exported as metrics, so pool starvation is visible on the dashboard.
- **Shutdown.** On `SIGTERM` the gRPC server does `GracefulStop()` with a timeout, then flushes the producer.

### Observability

- Every service sends traces, metrics and logs over OTLP to one OpenTelemetry Collector. Services know only the collector address.
- Traces go through gRPC, Kafka (the context travels in record headers), the consumer and Postgres, so one `hit_id` can be followed end to end in Tempo.
- The collector applies tail sampling: all traces with errors, all traces slower than 200 ms, and 5% of the rest. Metrics go to VictoriaMetrics, logs to Loki.
- Metrics include per-RPC latency by status code, open streams and slow-client disconnects, producer errors, batch size and duration, consumer lag, late and duplicate hits, outbox size, `pgxpool` stats and Go runtime stats.
- A Grafana dashboard is provisioned from the repository ([deploy/provisioning/dashboards/raidboss.json](deploy/provisioning/dashboards/raidboss.json)).

### Consistency checks

`raidctl verify` checks these invariants against Postgres and Redis. It runs after every load and chaos test.

1. For each defeated raid, the damage of accepted hits is at least `hp_max`, and without the killing blow it is less than `hp_max`.
2. Each defeated raid has exactly one `killer_hit_id`, and it exists in `raid_hits`.
3. No hit was applied after the killing blow of the same raid (checked by `seq`).
4. `raid_damage` equals the aggregate of `raid_hits`.
5. At most one reward per `(raid, player, kind)`, and only for defeated raids.
6. The Redis leaderboard equals `raid_damage` once the load has stopped.
7. At most one active raid per clan.

### API

`RaidService` (proto in [proto/raidboss/v1/raid.proto](proto/raidboss/v1/raid.proto)): `CreateRaid`, `GetRaid`, `Attack`, `GetLeaderboard`, `WatchRaid` (server streaming), `ListRewards`. Authentication is simplified: the player id comes from the `x-player-id` metadata.

| Situation | gRPC code |
|---|---|
| Missing or invalid `idempotency_key` | `INVALID_ARGUMENT` |
| Missing `x-player-id` | `UNAUTHENTICATED` |
| Raid or player not found | `NOT_FOUND` |
| Raid of another clan | `PERMISSION_DENIED` |
| Raid is not active | `FAILED_PRECONDITION` |
| Clan already has an active raid | `ALREADY_EXISTS` |
| Rate limit | `RESOURCE_EXHAUSTED` with `RetryInfo` |
| Queue unavailable | `UNAVAILABLE` |
| Client deadline passed | `DEADLINE_EXCEEDED` |

The server honours client deadlines: the request context goes to Redis, Postgres and the producer. It exposes `grpc.health.v1` (serving only while Postgres, Redis and Kafka are reachable) and, only when `GRPC_REFLECTION=true`, server reflection.

### Data model

PostgreSQL tables (migrations in [migrations/](migrations)): `clans`, `players`, `raids`, `raid_hits` (primary key `hit_id`, ordering `seq`), `raid_damage` (per-player aggregate), `rewards` (unique `(raid_id, player_id, kind)`), and `outbox` (topic, key, protobuf payload, headers, `sent_at`). `raidctl outbox dump` decodes the binary payload for debugging.

## The hot-row experiment

Question: if every hit updates the boss row in Postgres directly, what limits throughput first, the row lock or the connection pool? `ATTACK_MODE` switches `api` between three modes:

- `kafka` (default): the queue and the batching consumer described above.
- `sync`: one transaction in the request, `UPDATE raids … WHERE status='active'` plus `INSERT raid_hits … ON CONFLICT DO NOTHING`.
- `sync-full`: the same processor as the consumer, for a batch of one hit, inside the request.

Results (one machine, 30 s per step, client deadline 2 s, pool of 16, successful responses per second):

| Target RPS | kafka, one boss | sync, one boss | sync, 40 bosses | sync-full, one boss | sync-full, 40 bosses |
|---|---|---|---|---|---|
| 200 | – | 190 | 189 | 191 | 189 |
| 500 | 477 | 77 | 365 | 224 | 358 |
| 1000 | 956 | 53 | 665 | 160 | 367 |
| 4000 | 3820 | 246 | 719 | 156 | 503 |

- With one hot boss, the **row lock** is the limit and a bigger pool makes it worse: at 1000 RPS the `sync` mode gave 424 per second with a pool of 4 and 52 with a pool of 64.
- With the load spread over 40 bosses, the **pool** is the limit: a pool of 64 gives about 950 per second.
- `kafka` mode shows neither limit in this range: p99 stays at or below 44 ms up to 4000 RPS. Its price is that an answer means "accepted", not "applied".
- The `sync` modes do not plateau; above about 200–250 RPS they collapse, because requests wait for the lock longer than the client deadline and retry.

Charts, method and the full table of 74 runs are in [docs/hotrow/](docs/hotrow/README.md) (in Russian).

## Load test results

Open load model: a 10 s ramp, then 30 s steady, 40 clans × 50 players, about 5% repeated requests with the same key. The machine runs `botsim`, all services, Postgres, Kafka, Redis and monitoring together (Docker Desktop, 20 logical cores), so these numbers are a lower estimate for services on separate hosts. `raidctl verify` passes after every run.

| Target RPS | p50, ms | p95, ms | p99, ms | Errors | Max consumer lag |
|---|---|---|---|---|---|
| 1000 | 2.2 | 5.8 | 27.1 | 0 | 138 |
| 2000 | 2.7 | 7.6 | 14.2 | 0 | 301 |
| 4000 | 4.6 | 11.8 | 20.1 | 0 | 822 |
| 6000 | 10.3 | 33.0 | 51.6 | 1 | 2391 |
| 8000 | 47.2 | 142.6 | 188.7 | 0 | 5222 |

Latency grows slowly up to 4000 RPS. Between 4000 and 8000 RPS the system slows down without errors. What limits it first (`api`, the consumer, Postgres, or `botsim` on the same machine) was not determined.

- **1000 `WatchRaid` subscribers** at 1000 RPS: p99 of the main load is 23.1 ms, against 27.1 ms without subscribers. No reconnects, no slow-client disconnects.
- **One client connection against a pool of 8** at 6000 RPS: the pool was worse (p99 120 ms against 75 ms). The likely reason is that client and server share one machine; this was not tested further.
- **Chaos scenarios** (kill the consumer for 5 s at 1000 RPS, kill both relays for 25 s, restart Redis, stop Kafka for 30 s at 300 RPS): `raidctl verify` passes after each, and no hits or rewards are lost or duplicated. During the Kafka outage clients got `UNAVAILABLE` and no `DEADLINE_EXCEEDED`.

More detail, in Russian: [docs/spec.ru.md](docs/spec.ru.md), section 11.

## Run it

You need Docker, Go and a bash shell.

```bash
loadtest/demo.sh            # build, start infrastructure, migrate, seed, start services, run botsim + raidwatch
loadtest/demo.sh prepare    # only the environment
loadtest/demo.sh run        # botsim in the background, raidwatch on screen
```

The compose stack in `deploy/` starts Postgres, Kafka, Redis, the OpenTelemetry Collector, VictoriaMetrics, Tempo, Loki and Grafana. Settings come from environment variables; the example is in [deploy/.env.example](deploy/.env.example). The values there are demo values for a local stack.

Other scripts in `loadtest/`: `bench.sh` (the RPS ladder and connection tests), `hotrow.sh` and `hotrow_report.py` (the experiment), `chaos_*.sh` (failure scenarios).

Tests: `go test -race ./...` (integration tests use `testcontainers-go` and need Docker). CI runs `buf lint`, `buf breaking`, a check that generated code is up to date, `golangci-lint`, the build, and the tests.

## Repository layout

```
proto/       raidboss/v1 (public API), raidboss/events/v1 (Kafka messages)
gen/         generated Go code (committed, checked in CI)
cmd/         api, hits-consumer, rewards-worker, outbox-relay, scheduler, botsim, raidctl, raidwatch
internal/    domain (pure functions: damage, rewards), grpcapi, interceptors, raid, ratelimit,
             breaker, cache, storage/pg, storage/rdb, kafka, outbox, watch, telemetry,
             verify, rebuild, raidwatch
migrations/  SQL migrations (goose)
deploy/      docker-compose, OpenTelemetry Collector, Tempo, Loki, Grafana provisioning
loadtest/    load and chaos scripts
docs/        demo, experiment results, notes per development stage
```

Stack: Go, gRPC and protobuf (`buf`), PostgreSQL (`pgx/v5`, goose), Kafka (`franz-go`), Redis (`go-redis/v9`), OpenTelemetry, Grafana, Tempo, Loki, VictoriaMetrics, Docker Compose, GitHub Actions, `bubbletea` for the terminal client.

## Status and limitations

- This is a personal project. It has not been deployed to production and has no real users.
- All load is synthetic and comes from `botsim`. All measurements ran on one machine, with one run per data point.
- Authentication is a placeholder (a player id in metadata). In a real system it would be a token.
- Redis and Kafka in the compose stack run without authentication, with one broker and no replication.
- The documentation under `docs/` and the full specification in `docs/spec.ru.md` are in Russian.

## License

[MIT](LICENSE)
