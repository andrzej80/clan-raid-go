package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
)

// tokenBucketScript — token bucket одним атомарным вызовом.
//
// Время берётся из Redis (TIME): часы экземпляров api могут расходиться, а лимит общий.
// ARGV[3] — подмена «сейчас» в миллисекундах для тестов; в проде пустая строка.
// Токены хранятся в микротокенах (1 токен = 1e6), чтобы арифметика оставалась целочисленной
// и задержка считалась без ошибок округления double.
//
// KEYS[1] — ключ игрока; ARGV[1] — токенов в секунду, ARGV[2] — burst.
// Возвращает {1, 0} при разрешении и {0, retry_ms} при отказе.
var tokenBucketScript = redis.NewScript(`
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])

local now
if ARGV[3] ~= '' then
  now = tonumber(ARGV[3])
else
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local one      = 1000000
local capacity = burst * one
local per_ms   = rate * 1000 -- микротокенов за миллисекунду

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + elapsed * per_ms)

local allowed = 0
local retry   = 0
if tokens >= one then
  tokens = tokens - one
  allowed = 1
else
  retry = math.ceil((one - tokens) / per_ms)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
-- Полностью пополнившийся bucket неотличим от нового: ключ можно удалить.
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / per_ms) + 1000)
return {allowed, retry}
`)

const defaultRedisTimeout = 100 * time.Millisecond

// Redis — token bucket на игрока в Redis: лимит общий для всех экземпляров api.
type Redis struct {
	client  redis.Scripter
	rate    float64
	burst   int
	timeout time.Duration
	now     func() time.Time
}

type RedisOption func(*Redis)

// WithRedisClock заставляет скрипт использовать переданное время вместо TIME (для тестов).
func WithRedisClock(now func() time.Time) RedisOption {
	return func(r *Redis) { r.now = now }
}

// WithRedisTimeout ограничивает один вызов: недоступный Redis не должен добавлять латентность
// каждому удару (интерсептор при ошибке пропускает запрос).
func WithRedisTimeout(d time.Duration) RedisOption {
	return func(r *Redis) { r.timeout = d }
}

// NewRedis: perSecond запросов в секунду, всплеск до burst.
func NewRedis(client redis.Scripter, perSecond float64, burst int, opts ...RedisOption) *Redis {
	r := &Redis{client: client, rate: perSecond, burst: burst, timeout: defaultRedisTimeout}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Allow расходует один токен игрока. Если токенов нет, возвращает, через сколько появится следующий.
func (r *Redis) Allow(ctx context.Context, playerID int64) (bool, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	nowArg := ""
	if r.now != nil {
		nowArg = strconv.FormatInt(r.now().UnixMilli(), 10)
	}
	res, err := tokenBucketScript.Run(ctx, r.client,
		[]string{rdb.RateLimitKey(playerID)}, r.rate, r.burst, nowArg).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("rate limit script: %w", err)
	}
	if len(res) != 2 {
		return false, 0, fmt.Errorf("rate limit script: unexpected reply %v", res)
	}
	if res[0] == 1 {
		return true, 0, nil
	}
	return false, time.Duration(res[1]) * time.Millisecond, nil
}
