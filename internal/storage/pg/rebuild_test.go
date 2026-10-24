package pg_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/raid"
	"github.com/andrzej80/clan-raid-go/internal/rebuild"
	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
	"github.com/andrzej80/clan-raid-go/internal/verify"
)

// newBoardEnv — процессор, пишущий в настоящий Redis через rdb.Board.
func newBoardEnv(t *testing.T, players int) (*processorEnv, *redis.Client) {
	t.Helper()
	rc := requireRedis(t)
	return newProcessorEnv(t, players, raid.WithBoard(rdb.NewBoard(rc))), rc
}

func verifyRedis(t *testing.T, env *processorEnv, rc *redis.Client, raidID uuid.UUID) []verify.Violation {
	t.Helper()
	v, err := verify.RunRedis(context.Background(), env.pool, rc, []uuid.UUID{raidID})
	if err != nil {
		t.Fatalf("verify.RunRedis: %v", err)
	}
	return v
}

func rebuildRaid(t *testing.T, env *processorEnv, rc *redis.Client, raidID uuid.UUID, opts rebuild.Options) rebuild.Result {
	t.Helper()
	res, err := rebuild.Run(context.Background(), env.pool, rc, []uuid.UUID{raidID}, opts)
	if err != nil {
		t.Fatalf("rebuild.Run: %v", err)
	}
	return res
}

// dropRaidKeys имитирует FLUSHALL для одного рейда: тесты делят Redis и идут параллельно.
func dropRaidKeys(t *testing.T, rc *redis.Client, raidID uuid.UUID) {
	t.Helper()
	if err := rc.Del(context.Background(), rdb.LeaderboardKey(raidID), rdb.HPKey(raidID)).Err(); err != nil {
		t.Fatalf("drop keys: %v", err)
	}
}

func hit5(env *processorEnv, raidID uuid.UUID) []domain.Hit {
	hits := make([]domain.Hit, len(env.players))
	for i, p := range env.players {
		hits[i] = env.hit(raidID, p, 100*(i+1))
	}
	return hits
}

// Критерий этапа: после потери Redis и rebuild-cache инвариант 6 выполняется.
func TestRebuild_RestoresInvariant6AfterRedisLoss(t *testing.T) {
	t.Parallel()

	env, rc := newBoardEnv(t, 5)
	r := env.newRaidWithHP(t, 100_000)
	mustProcess(t, env.processor, hit5(env, r.ID))

	if v := verifyRedis(t, env, rc, r.ID); len(v) != 0 {
		t.Fatalf("redis differs right after processing: %v", v)
	}

	dropRaidKeys(t, rc, r.ID)
	v := verifyRedis(t, env, rc, r.ID)
	if !invariants(v)[6] || !invariants(v)[0] {
		t.Fatalf("verify must notice the lost leaderboard and hp, got %v", v)
	}

	res := rebuildRaid(t, env, rc, r.ID, rebuild.Options{})
	if res.Raids != 1 || res.Players != 5 {
		t.Fatalf("result = %+v, want 1 raid and 5 players", res)
	}
	if v := verifyRedis(t, env, rc, r.ID); len(v) != 0 {
		t.Fatalf("violations after rebuild: %v", v)
	}
}

// Мусор в Redis (лишний игрок, неверные счета, неверное HP) заменяется данными Postgres.
func TestRebuild_ReplacesStaleData(t *testing.T) {
	t.Parallel()

	env, rc := newBoardEnv(t, 3)
	r := env.newRaidWithHP(t, 100_000)
	mustProcess(t, env.processor, hit5(env, r.ID))

	ctx := context.Background()
	rc.ZAdd(ctx, rdb.LeaderboardKey(r.ID), redis.Z{Score: 999_999, Member: "424242"}) // лишний
	rc.ZAdd(ctx, rdb.LeaderboardKey(r.ID), redis.Z{Score: 1, Member: "1"})
	rc.Set(ctx, rdb.HPKey(r.ID), 7, 0)
	if v := verifyRedis(t, env, rc, r.ID); len(v) == 0 {
		t.Fatal("verify must notice the garbage")
	}

	rebuildRaid(t, env, rc, r.ID, rebuild.Options{})
	if v := verifyRedis(t, env, rc, r.ID); len(v) != 0 {
		t.Fatalf("violations after rebuild: %v", v)
	}
}

func TestRebuild_IsIdempotent(t *testing.T) {
	t.Parallel()

	env, rc := newBoardEnv(t, 4)
	r := env.newRaidWithHP(t, 100_000)
	mustProcess(t, env.processor, hit5(env, r.ID))

	for range 3 {
		rebuildRaid(t, env, rc, r.ID, rebuild.Options{})
	}
	if v := verifyRedis(t, env, rc, r.ID); len(v) != 0 {
		t.Fatalf("violations after repeated rebuild: %v", v)
	}
}

// Лидерборд собирается пачками по ChunkSize, и ни одного игрока на границе не теряется.
func TestRebuild_Chunking(t *testing.T) {
	t.Parallel()

	env, rc := newBoardEnv(t, 7)
	r := env.newRaidWithHP(t, 100_000)
	mustProcess(t, env.processor, hit5(env, r.ID))
	dropRaidKeys(t, rc, r.ID)

	rebuildRaid(t, env, rc, r.ID, rebuild.Options{ChunkSize: 3})
	if v := verifyRedis(t, env, rc, r.ID); len(v) != 0 {
		t.Fatalf("violations after chunked rebuild: %v", v)
	}
}

// Рейд без ударов: лидерборда нет (остатки удаляются), HP записан.
func TestRebuild_RaidWithoutDamage(t *testing.T) {
	t.Parallel()

	env, rc := newBoardEnv(t, 1)
	r := env.newRaidWithHP(t, 1000)
	rc.ZAdd(context.Background(), rdb.LeaderboardKey(r.ID), redis.Z{Score: 5, Member: "1"})

	res := rebuildRaid(t, env, rc, r.ID, rebuild.Options{})
	if res.Raids != 1 || res.Players != 0 {
		t.Fatalf("result = %+v, want 1 raid and 0 players", res)
	}
	if v := verifyRedis(t, env, rc, r.ID); len(v) != 0 {
		t.Fatalf("violations after rebuild: %v", v)
	}
	if n := rc.Exists(context.Background(), rdb.LeaderboardKey(r.ID)).Val(); n != 0 {
		t.Fatal("leaderboard key must not exist for a raid nobody hit")
	}
}

// После rebuild не остаётся временных ключей, а у лидерборда нет TTL от временного ключа.
func TestRebuild_NoLeftoversAndNoTTL(t *testing.T) {
	t.Parallel()

	env, rc := newBoardEnv(t, 3)
	r := env.newRaidWithHP(t, 100_000)
	mustProcess(t, env.processor, hit5(env, r.ID))
	rebuildRaid(t, env, rc, r.ID, rebuild.Options{})

	ctx := context.Background()
	keys, err := rc.Keys(ctx, rdb.LeaderboardKey(r.ID)+"*").Result()
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	if len(keys) != 1 || strings.Contains(keys[0], "rebuild") {
		t.Fatalf("keys = %v, want only the leaderboard", keys)
	}
	if ttl := rc.TTL(ctx, rdb.LeaderboardKey(r.ID)).Val(); ttl > 0 {
		t.Fatalf("leaderboard ttl = %v, want none", ttl)
	}
}

func TestRebuild_DoesNotTouchOtherKeys(t *testing.T) {
	t.Parallel()

	env, rc := newBoardEnv(t, 1)
	r := env.newRaidWithHP(t, 1000)
	ctx := context.Background()
	key := rdb.RateLimitKey(987_654_321)
	rc.HSet(ctx, key, "tokens", 1)

	rebuildRaid(t, env, rc, r.ID, rebuild.Options{})
	if rc.Exists(ctx, key).Val() != 1 {
		t.Fatal("rebuild must not remove rate limit keys")
	}
}

// Без фильтра по рейду обрабатываются все рейды базы.
// Не параллельный: перестраивает кэш всех рейдов базы, то есть и рейдов соседних тестов, и сломал бы их проверки.
func TestRebuild_AllRaids(t *testing.T) { //nolint:paralleltest // см. комментарий выше
	env, rc := newBoardEnv(t, 2)
	r := env.newRaidWithHP(t, 100_000)
	mustProcess(t, env.processor, hit5(env, r.ID))
	dropRaidKeys(t, rc, r.ID)

	res, err := rebuild.Run(context.Background(), env.pool, rc, nil, rebuild.Options{})
	if err != nil {
		t.Fatalf("rebuild.Run: %v", err)
	}
	if res.Raids < 1 {
		t.Fatalf("result = %+v, want at least this test's raid", res)
	}
	if v := verifyRedis(t, env, rc, r.ID); len(v) != 0 {
		t.Fatalf("violations after full rebuild: %v", v)
	}
}

func TestVerifyRedis_DetectsDifferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		corrupt func(ctx context.Context, rc *redis.Client, raidID uuid.UUID, player int64)
		wantInv int
	}{
		{"player missing", func(ctx context.Context, rc *redis.Client, id uuid.UUID, p int64) {
			rc.ZRem(ctx, rdb.LeaderboardKey(id), p)
		}, 6},
		{"wrong score", func(ctx context.Context, rc *redis.Client, id uuid.UUID, p int64) {
			rc.ZAdd(ctx, rdb.LeaderboardKey(id), redis.Z{Score: 1, Member: p})
		}, 6},
		{"extra player", func(ctx context.Context, rc *redis.Client, id uuid.UUID, _ int64) {
			rc.ZAdd(ctx, rdb.LeaderboardKey(id), redis.Z{Score: 1, Member: "424242"})
		}, 6},
		{"wrong hp", func(ctx context.Context, rc *redis.Client, id uuid.UUID, _ int64) {
			rc.Set(ctx, rdb.HPKey(id), 1, 0)
		}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env, rc := newBoardEnv(t, 2)
			r := env.newRaidWithHP(t, 100_000)
			mustProcess(t, env.processor, hit5(env, r.ID))
			if v := verifyRedis(t, env, rc, r.ID); len(v) != 0 {
				t.Fatalf("violations before corruption: %v", v)
			}

			tt.corrupt(context.Background(), rc, r.ID, env.players[0])
			v := verifyRedis(t, env, rc, r.ID)
			if len(v) != 1 || v[0].Invariant != tt.wantInv {
				t.Fatalf("violations = %v, want exactly one with invariant %d", v, tt.wantInv)
			}
		})
	}
}
