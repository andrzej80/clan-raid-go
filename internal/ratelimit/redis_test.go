package ratelimit

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
	"github.com/andrzej80/clan-raid-go/internal/testutil"
)

// testRedis общий для тестов пакета; nil, если Docker недоступен.
var testRedis *redis.Client

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	client, cleanup, err := testutil.StartRedis(context.Background())
	if err != nil {
		// В CI Docker обязателен: иначе интеграционные тесты молча пропустятся, а сборка останется зелёной.
		if os.Getenv("REQUIRE_DOCKER") == "true" {
			fmt.Fprintln(os.Stderr, "redis unavailable, but REQUIRE_DOCKER=true:", err)
			return 1
		}
		fmt.Fprintln(os.Stderr, "redis unavailable, integration tests will be skipped:", err)
		return m.Run()
	}
	defer cleanup()
	testRedis = client
	return m.Run()
}

func requireRedis(t *testing.T) *redis.Client {
	t.Helper()
	if testRedis == nil {
		t.Skip("docker is not available")
	}
	return testRedis
}

var playerSeq atomic.Int64

// newPlayerID даёт свежий ключ: тесты делят один Redis и идут параллельно.
func newPlayerID() int64 { return 1_000_000 + playerSeq.Add(1) }

func newRedisLimiter(t *testing.T) (*Redis, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)}
	return NewRedis(requireRedis(t), 5, 10, WithRedisClock(clock.Now)), clock
}

func redisAllow(t *testing.T, r *Redis, player int64) (bool, time.Duration) {
	t.Helper()
	ok, retry, err := r.Allow(context.Background(), player)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return ok, retry
}

// Правило 4 из README: 5 ударов в секунду, burst 10. Время подменено, поэтому результат точный.
func TestRedis_BurstThenRate(t *testing.T) {
	t.Parallel()
	r, clock := newRedisLimiter(t)
	player := newPlayerID()

	for i := range 10 {
		if ok, _ := redisAllow(t, r, player); !ok {
			t.Fatalf("request %d of the burst was rejected", i+1)
		}
	}
	ok, retry := redisAllow(t, r, player)
	if ok {
		t.Fatal("11th request in the same instant must be rejected")
	}
	if retry != 200*time.Millisecond {
		t.Fatalf("retry after = %v, want 200ms (1/5 s)", retry)
	}

	clock.Advance(time.Second) // за секунду накапало 5 токенов
	for i := range 5 {
		if ok, _ := redisAllow(t, r, player); !ok {
			t.Fatalf("request %d after refill was rejected", i+1)
		}
	}
	if ok, _ := redisAllow(t, r, player); ok {
		t.Fatal("6th request after one second must be rejected")
	}
}

func TestRedis_RetryAfterShrinksWithTime(t *testing.T) {
	t.Parallel()
	r, clock := newRedisLimiter(t)
	player := newPlayerID()
	for range 10 {
		redisAllow(t, r, player)
	}

	clock.Advance(50 * time.Millisecond)
	ok, retry := redisAllow(t, r, player)
	if ok || retry != 150*time.Millisecond {
		t.Fatalf("ok=%v retry=%v, want rejected with 150ms", ok, retry)
	}

	clock.Advance(150 * time.Millisecond) // ровно 200 мс с момента опустошения
	if ok, _ := redisAllow(t, r, player); !ok {
		t.Fatal("request after exactly 200ms must pass")
	}
	if ok, _ := redisAllow(t, r, player); ok {
		t.Fatal("only one token must have accumulated in 200ms")
	}
}

func TestRedis_RejectionDoesNotSpendToken(t *testing.T) {
	t.Parallel()
	r, clock := newRedisLimiter(t)
	player := newPlayerID()
	for range 10 {
		redisAllow(t, r, player)
	}
	for range 20 { // шквал отказов в тот же момент
		redisAllow(t, r, player)
	}

	clock.Advance(200 * time.Millisecond)
	if ok, _ := redisAllow(t, r, player); !ok {
		t.Fatal("rejected requests must not push the next token further away")
	}
}

func TestRedis_RefillIsCappedAtBurst(t *testing.T) {
	t.Parallel()
	r, clock := newRedisLimiter(t)
	player := newPlayerID()
	for range 10 {
		redisAllow(t, r, player)
	}

	clock.Advance(time.Hour)
	for i := range 10 {
		if ok, _ := redisAllow(t, r, player); !ok {
			t.Fatalf("request %d after a long pause was rejected", i+1)
		}
	}
	if ok, _ := redisAllow(t, r, player); ok {
		t.Fatal("bucket must not hold more than burst tokens")
	}
}

func TestRedis_PlayersAreIndependent(t *testing.T) {
	t.Parallel()
	r, _ := newRedisLimiter(t)
	a, b := newPlayerID(), newPlayerID()
	for range 10 {
		redisAllow(t, r, a)
	}
	if ok, _ := redisAllow(t, r, a); ok {
		t.Fatal("player a must be limited")
	}
	if ok, _ := redisAllow(t, r, b); !ok {
		t.Fatal("player b must not be affected by player a")
	}
}

func TestRedis_KeyExpires(t *testing.T) {
	t.Parallel()
	r, _ := newRedisLimiter(t)
	player := newPlayerID()
	redisAllow(t, r, player)

	ttl, err := requireRedis(t).PTTL(context.Background(), rdb.RateLimitKey(player)).Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	// Полное пополнение занимает 2 с (10 токенов по 5/с) плюс запас.
	if ttl <= 0 || ttl > 4*time.Second {
		t.Fatalf("ttl = %v, want in (0, 4s]", ttl)
	}
}

// Лимит общий для всех экземпляров api: 50 параллельных вызовов одного игрока
// в один момент пропускают ровно burst запросов.
func TestRedis_ConcurrentRequestsRespectBurst(t *testing.T) {
	t.Parallel()
	r, _ := newRedisLimiter(t)
	player := newPlayerID()

	var allowed atomic.Int32
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if ok, _, err := r.Allow(context.Background(), player); err == nil && ok {
				allowed.Add(1)
			}
		})
	}
	wg.Wait()
	if got := allowed.Load(); got != 10 {
		t.Fatalf("allowed = %d, want exactly burst (10)", got)
	}
}

// Без подмены часов время берётся из Redis (TIME): после burst отказ с задержкой не больше 200 мс.
func TestRedis_UsesServerTimeByDefault(t *testing.T) {
	t.Parallel()
	r := NewRedis(requireRedis(t), 5, 10)
	player := newPlayerID()
	for range 10 {
		redisAllow(t, r, player)
	}
	ok, retry := redisAllow(t, r, player)
	if ok {
		t.Fatal("11th request must be rejected")
	}
	if retry <= 0 || retry > 200*time.Millisecond {
		t.Fatalf("retry = %v, want in (0, 200ms]", retry)
	}
}

func TestRedis_ErrorWhenRedisUnavailable(t *testing.T) {
	t.Parallel()
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond, MaxRetries: -1})
	defer func() { _ = dead.Close() }()

	r := NewRedis(dead, 5, 10, WithRedisTimeout(200*time.Millisecond))
	if _, _, err := r.Allow(context.Background(), 1); err == nil {
		t.Fatal("Allow must return an error when Redis is down")
	}
}

// ceilMs округляет вверх до миллисекунды, как это делает интерсептор перед отдачей RetryInfo.
func ceilMs(d time.Duration) time.Duration {
	return (d + time.Millisecond - 1).Truncate(time.Millisecond)
}

// Контракт Limiter: Memory и Redis ведут себя одинаково на одной и той же последовательности.
func TestLimiters_SameBehavior(t *testing.T) {
	t.Parallel()
	redisLimiter, redisClock := newRedisLimiter(t)
	mem, memClock := newTestLimiter()

	steps := []struct {
		advance time.Duration
		repeat  int
	}{
		{0, 12}, {30 * time.Millisecond, 1}, {170 * time.Millisecond, 2}, {time.Second, 7}, {3 * time.Second, 12},
	}
	player := newPlayerID()
	for i, s := range steps {
		redisClock.Advance(s.advance)
		memClock.Advance(s.advance)
		for j := range s.repeat {
			okR, retryR, errR := redisLimiter.Allow(context.Background(), player)
			okM, retryM, errM := mem.Allow(context.Background(), 1)
			if errR != nil || errM != nil {
				t.Fatalf("step %d/%d: errors redis=%v memory=%v", i, j, errR, errM)
			}
			if okR != okM || ceilMs(retryR) != ceilMs(retryM) {
				t.Fatalf("step %d/%d: redis=(%v,%v) memory=(%v,%v)", i, j, okR, retryR, okM, retryM)
			}
		}
	}
}
