package main

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/grpcapi"
	"github.com/andrzej80/clan-raid-go/internal/interceptors"
	"github.com/andrzej80/clan-raid-go/internal/raid"
	"github.com/andrzej80/clan-raid-go/internal/ratelimit"
)

type fakePlayers map[int64]*domain.Player

func (f fakePlayers) GetPlayer(_ context.Context, id int64) (*domain.Player, error) {
	p, ok := f[id]
	if !ok {
		return nil, domain.ErrPlayerNotFound
	}
	cp := *p
	return &cp, nil
}

type recordingPublisher struct {
	mu    sync.Mutex
	hits  []domain.Hit
	err   error
	block bool // ждать отмены контекста, как зависший брокер
}

func (p *recordingPublisher) Publish(ctx context.Context, hit domain.Hit) error {
	if p.block {
		<-ctx.Done()
		return ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.hits = append(p.hits, hit)
	return nil
}

func (p *recordingPublisher) published() []domain.Hit {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]domain.Hit(nil), p.hits...)
}

type attackEnv struct {
	client    raidbossV1.RaidServiceClient
	raids     *fakeRaids
	publisher *recordingPublisher
	raidID    uuid.UUID
}

const (
	attackerID    = 10
	attackerClan  = 1
	attackerPower = 500
)

func newAttackEnv(t *testing.T, limiter interceptors.Limiter) *attackEnv {
	t.Helper()

	raids := newFakeRaids(attackerClan)
	raidID := uuid.New()
	raids.raids[raidID] = domain.Raid{
		ID: raidID, ClanID: attackerClan, BossName: "Dragon", HPMax: 1000, HPLeft: 1000,
		Status: domain.RaidActive, StartedAt: time.Now().Add(-time.Hour), EndsAt: time.Now().Add(time.Hour),
	}
	players := fakePlayers{
		attackerID: {ID: attackerID, ClanID: attackerClan, Name: "p", Power: attackerPower},
		11:         {ID: 11, ClanID: 2, Name: "stranger", Power: 100},
		12:         {ID: 12, ClanID: attackerClan, Name: "p2", Power: 100},
	}
	publisher := &recordingPublisher{}

	svc := grpcapi.NewRaidService(
		grpcapi.WithRaids(grpcapi.NewRaidHandlers(raid.NewService(raids))),
		grpcapi.WithAttacks(grpcapi.NewAttackHandlers(raid.NewAttackService(raids, players, publisher))),
	)
	conn := startTestServerWithLimiter(t, &config.Config{}, svc, limiter)
	return &attackEnv{
		client:    raidbossV1.NewRaidServiceClient(conn),
		raids:     raids,
		publisher: publisher,
		raidID:    raidID,
	}
}

func (e *attackEnv) request(key string) *raidbossV1.AttackRequest {
	return &raidbossV1.AttackRequest{RaidId: e.raidID.String(), IdempotencyKey: key}
}

func playerCtx(t *testing.T, playerID int64) context.Context {
	t.Helper()

	// Не testCtx: он уже подставляет x-player-id, а auth читает первое значение заголовка.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return metadata.AppendToOutgoingContext(ctx, interceptors.PlayerIDHeader, strconv.FormatInt(playerID, 10))
}

func TestAPI_AttackAccepted(t *testing.T) {
	t.Parallel()
	env := newAttackEnv(t, allowAll{})
	key := uuid.NewString()

	resp, err := env.client.Attack(playerCtx(t, attackerID), env.request(key))
	if err != nil {
		t.Fatalf("Attack: %v", err)
	}

	if resp.GetHitId() != key {
		t.Fatalf("hit_id = %q, want idempotency key %q", resp.GetHitId(), key)
	}
	lo, hi := int32(400), int32(600)
	if resp.GetIsCrit() {
		lo, hi = 800, 1200
	}
	if resp.GetDamage() < lo || resp.GetDamage() > hi {
		t.Fatalf("damage %d (crit=%t) out of [%d, %d]", resp.GetDamage(), resp.GetIsCrit(), lo, hi)
	}

	hits := env.publisher.published()
	if len(hits) != 1 || hits[0].PlayerID != attackerID || hits[0].RaidID != env.raidID ||
		int32(hits[0].Damage) != resp.GetDamage() {
		t.Fatalf("published hits = %+v, want one matching the response", hits)
	}
}

// Повтор с тем же ключом (например, после UNAVAILABLE) возвращает тот же удар.
func TestAPI_AttackRetryReturnsSameHit(t *testing.T) {
	t.Parallel()
	env := newAttackEnv(t, allowAll{})
	key := uuid.NewString()

	first, err := env.client.Attack(playerCtx(t, attackerID), env.request(key))
	if err != nil {
		t.Fatalf("first Attack: %v", err)
	}
	second, err := env.client.Attack(playerCtx(t, attackerID), env.request(key))
	if err != nil {
		t.Fatalf("second Attack: %v", err)
	}

	if first.GetHitId() != second.GetHitId() || first.GetDamage() != second.GetDamage() ||
		first.GetIsCrit() != second.GetIsCrit() {
		t.Fatalf("retry returned %v, first was %v", second, first)
	}
}

func TestAPI_AttackInvalidArguments(t *testing.T) {
	t.Parallel()
	env := newAttackEnv(t, allowAll{})

	tests := []struct {
		name string
		req  *raidbossV1.AttackRequest
	}{
		{"raid id not a uuid", &raidbossV1.AttackRequest{RaidId: "nope", IdempotencyKey: uuid.NewString()}},
		{"empty raid id", &raidbossV1.AttackRequest{IdempotencyKey: uuid.NewString()}},
		{"no idempotency key", env.request("")},
		{"idempotency key not a uuid", env.request("123")},
		{"nil idempotency key", env.request(uuid.Nil.String())},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := env.client.Attack(playerCtx(t, attackerID), tc.req)
			requireCode(t, err, codes.InvalidArgument)
		})
	}
	if n := len(env.publisher.published()); n != 0 {
		t.Fatalf("invalid requests must not be published, got %d", n)
	}
}

func TestAPI_AttackRejections(t *testing.T) {
	t.Parallel()

	t.Run("unknown player", func(t *testing.T) {
		t.Parallel()
		env := newAttackEnv(t, allowAll{})
		_, err := env.client.Attack(playerCtx(t, 999), env.request(uuid.NewString()))
		requireCode(t, err, codes.NotFound)
	})
	t.Run("unknown raid", func(t *testing.T) {
		t.Parallel()
		env := newAttackEnv(t, allowAll{})
		req := &raidbossV1.AttackRequest{RaidId: uuid.NewString(), IdempotencyKey: uuid.NewString()}
		_, err := env.client.Attack(playerCtx(t, attackerID), req)
		requireCode(t, err, codes.NotFound)
	})
	t.Run("raid of another clan", func(t *testing.T) {
		t.Parallel()
		env := newAttackEnv(t, allowAll{})
		_, err := env.client.Attack(playerCtx(t, 11), env.request(uuid.NewString()))
		requireCode(t, err, codes.PermissionDenied)
	})
	t.Run("raid not active", func(t *testing.T) {
		t.Parallel()
		env := newAttackEnv(t, allowAll{})
		finished := env.raids.raids[env.raidID]
		finished.Status = domain.RaidDefeated
		env.raids.raids[env.raidID] = finished

		_, err := env.client.Attack(playerCtx(t, attackerID), env.request(uuid.NewString()))
		requireCode(t, err, codes.FailedPrecondition)
	})
}

// Kafka недоступна: UNAVAILABLE с фиксированным текстом, детали брокера клиенту не видны.
func TestAPI_AttackQueueUnavailable(t *testing.T) {
	t.Parallel()
	env := newAttackEnv(t, allowAll{})
	env.publisher.err = errors.New("kafka: dial tcp 10.0.0.5:9092: connection refused")

	_, err := env.client.Attack(playerCtx(t, attackerID), env.request(uuid.NewString()))
	requireCode(t, err, codes.Unavailable)
	if msg := status.Convert(err).Message(); msg != "hit queue unavailable" {
		t.Fatalf("message = %q, broker details must not leak", msg)
	}
}

// Критерий этапа 2: клиент с дедлайном 1 мс получает DEADLINE_EXCEEDED, а не зависание.
func TestAPI_AttackClientDeadline(t *testing.T) {
	t.Parallel()
	env := newAttackEnv(t, allowAll{})
	env.publisher.block = true

	ctx, cancel := context.WithTimeout(
		metadata.AppendToOutgoingContext(context.Background(), interceptors.PlayerIDHeader, "10"), time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := env.client.Attack(ctx, env.request(uuid.NewString()))
	requireCode(t, err, codes.DeadlineExceeded)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Attack hung for %v", elapsed)
	}
}

// Правило 4: 5 ударов в секунду, burst 10; сверх лимита RESOURCE_EXHAUSTED с RetryInfo.
func TestAPI_AttackRateLimit(t *testing.T) {
	t.Parallel()
	env := newAttackEnv(t, ratelimit.NewMemory(5, 10))

	for i := range 10 {
		if _, err := env.client.Attack(playerCtx(t, attackerID), env.request(uuid.NewString())); err != nil {
			t.Fatalf("attack %d within burst: %v", i+1, err)
		}
	}

	_, err := env.client.Attack(playerCtx(t, attackerID), env.request(uuid.NewString()))
	requireCode(t, err, codes.ResourceExhausted)

	var retry time.Duration
	for _, d := range status.Convert(err).Details() {
		if info, ok := d.(*errdetails.RetryInfo); ok {
			retry = info.GetRetryDelay().AsDuration()
		}
	}
	if retry <= 0 || retry > 200*time.Millisecond {
		t.Fatalf("retry delay = %v, want within (0, 200ms]", retry)
	}

	// Лимит персональный: другой игрок клана бьёт свободно.
	if _, err := env.client.Attack(playerCtx(t, 12), env.request(uuid.NewString())); err != nil {
		t.Fatalf("other player must not be limited: %v", err)
	}
}
