package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

const (
	rpcTimeout   = 2 * time.Second
	maxAttempts  = 5
	retryBackoff = 100 * time.Millisecond
)

type player struct {
	id     int64
	clanID int64
}

type simConfig struct {
	clans        int
	players      int // игроков на клан
	rate         float64
	dupProb      float64
	hp           int64
	raidDuration time.Duration

	// open model: запросы выпускаются по расписанию, независимо от скорости ответов.
	open        bool
	targetRPS   float64
	rampUp      time.Duration
	maxInflight int

	watchers int // подписчиков WatchRaid, раздаются по рейдам по кругу

	retryRatio float64      // доля повторов от новых запросов; < 0 — без ограничения
	budget     *retryBudget // общий бюджет повторов, создаётся в simulate
}

// loadPlayers читает из БД первые clans кланов с игроками и до players игроков в каждом.
func loadPlayers(ctx context.Context, pool *pgxpool.Pool, clans, players int) (map[int64][]player, error) {
	rows, err := pool.Query(ctx, `
		SELECT clan_id, id FROM players
		WHERE clan_id IN (SELECT DISTINCT clan_id FROM players ORDER BY clan_id LIMIT $1)
		ORDER BY clan_id, id`, clans)
	if err != nil {
		return nil, fmt.Errorf("select players: %w", err)
	}
	defer rows.Close()

	byClan := make(map[int64][]player)
	for rows.Next() {
		var p player
		if err := rows.Scan(&p.clanID, &p.id); err != nil {
			return nil, err
		}
		if len(byClan[p.clanID]) < players {
			byClan[p.clanID] = append(byClan[p.clanID], p)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(byClan) == 0 {
		return nil, errors.New("no players in DB: run raidctl seed first")
	}
	return byClan, nil
}

// ensureRaid возвращает id активного рейда клана; если его нет — создаёт через gRPC.
func ensureRaid(ctx context.Context, pool *pgxpool.Pool, client raidbossV1.RaidServiceClient,
	creator player,
	cfg simConfig,
) (string, error) {
	var id string
	err := pool.QueryRow(ctx, `SELECT id::text FROM raids WHERE clan_id = $1 AND status = 'active'`, creator.clanID).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("select active raid: %w", err)
	}

	resp, err := client.CreateRaid(withPlayer(ctx, creator.id), &raidbossV1.CreateRaidRequest{
		ClanId: creator.clanID, BossName: fmt.Sprintf("boss-%d", creator.clanID), HpMax: cfg.hp,
		DurationSec: int32(cfg.raidDuration.Seconds()),
	})
	if err != nil {
		return "", fmt.Errorf("create raid for clan %d: %w", creator.clanID, err)
	}
	return resp.GetRaid().GetId(), nil
}

func withPlayer(ctx context.Context, playerID int64) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-player-id", strconv.FormatInt(playerID, 10))
}

// runBot бьёт по рейду, пока не истечёт ctx или рейд не перестанет быть активным.
func runBot(ctx context.Context, conn grpc.ClientConnInterface, p player, raidID string, cfg simConfig, st *stats) {
	client := raidbossV1.NewRaidServiceClient(conn)
	interval := time.Duration(float64(time.Second) / cfg.rate)

	for {
		// Джиттер ±20%, чтобы боты не стучались в одну миллисекунду.
		pause := time.Duration(float64(interval) * (0.8 + 0.4*rand.Float64())) //nolint:gosec // джиттер, не криптография
		select {
		case <-ctx.Done():
			return
		case <-time.After(pause):
		}

		key := uuid.NewString()
		code := attack(ctx, client, p, raidID, key, st, cfg.budget)
		if code == codes.FailedPrecondition || code == codes.NotFound {
			return // рейд побеждён или истёк
		}
		if code == codes.OK && rand.Float64() < cfg.dupProb { //nolint:gosec // доля дублей
			attack(ctx, client, p, raidID, key, st, cfg.budget) // повтор с тем же ключом: второго удара быть не должно
		}
	}
}

// attack отправляет удар, повторяя при UNAVAILABLE/DEADLINE_EXCEEDED/RESOURCE_EXHAUSTED с тем же
// idempotency_key. Пауза между повторами растёт, имеет джиттер и не меньше RetryInfo сервера; общее число
// повторов ограничено бюджетом rb (nil — без ограничения). Каждая попытка попадает в статистику отдельно.
// Возвращает код последней попытки.
func attack(
	ctx context.Context, client raidbossV1.RaidServiceClient, p player, raidID, key string, st *stats, rb *retryBudget,
) codes.Code {
	var code codes.Code
	for attempt := range maxAttempts {
		if attempt == 0 {
			rb.deposit()
		}
		callCtx, cancel := context.WithTimeout(withPlayer(ctx, p.id), rpcTimeout)
		start := time.Now()
		_, err := client.Attack(callCtx, &raidbossV1.AttackRequest{RaidId: raidID, IdempotencyKey: key})
		cancel()
		if ctx.Err() != nil {
			return codes.Canceled // остановка симуляции не считается результатом сервера
		}

		code = status.Code(err)
		st.record(code, time.Since(start))
		if code == codes.OK {
			st.markAccepted()
		}
		if !retryable(code) || attempt == maxAttempts-1 {
			return code
		}
		if !rb.withdraw() {
			st.markRetryDenied()
			return code
		}
		st.markRetry()
		select {
		case <-ctx.Done():
			return codes.Canceled
		case <-time.After(retryDelay(attempt, err)):
		}
	}
	return code
}

func retryable(c codes.Code) bool {
	return c == codes.Unavailable || c == codes.DeadlineExceeded || c == codes.ResourceExhausted
}

// target — игрок и рейд его клана.
type target struct {
	p      player
	raidID string
	dead   atomic.Bool // рейд побеждён или истёк: удары по нему больше не шлём
}

// prepare загружает игроков и гарантирует активный рейд для каждого клана.
func prepare(ctx context.Context, pool *pgxpool.Pool, conn grpc.ClientConnInterface, cfg simConfig) ([]*target, []string, error) {
	byClan, err := loadPlayers(ctx, pool, cfg.clans, cfg.players)
	if err != nil {
		return nil, nil, err
	}
	client := raidbossV1.NewRaidServiceClient(conn)

	var (
		targets []*target
		raidIDs []string
	)
	for _, players := range byClan {
		raidID, err := ensureRaid(ctx, pool, client, players[0], cfg)
		if err != nil {
			return nil, nil, err
		}
		raidIDs = append(raidIDs, raidID)
		for _, p := range players {
			targets = append(targets, &target{p: p, raidID: raidID})
		}
	}
	return targets, raidIDs, nil
}

// simulate запускает нагрузку в выбранной модели (и подписчиков, если они заказаны) и возвращает
// объединённую статистику и список рейдов.
func simulate(ctx context.Context, pool *pgxpool.Pool, conn grpc.ClientConnInterface, cfg simConfig) (*stats, []string, error) {
	cfg.budget = newRetryBudget(cfg.retryRatio)
	targets, raidIDs, err := prepare(ctx, pool, conn, cfg)
	if err != nil {
		return nil, nil, err
	}

	var (
		ws       *watchStats
		watchers sync.WaitGroup
		stop     = func() {}
	)
	if cfg.watchers > 0 {
		var wctx context.Context
		wctx, stop = context.WithCancel(ctx)
		ws = newWatchStats()
		watchers.Go(func() { runWatchers(wctx, conn, targets, cfg.watchers, ws) })
	}

	total := runLoad(ctx, conn, targets, cfg, time.Now())

	stop() // подписчики, не дождавшиеся finished, выходят по отмене
	watchers.Wait()
	total.watch = ws
	return total, raidIDs, nil
}

// runLoad гонит нагрузку в выбранной модели до конца ctx или до завершения всех рейдов.
func runLoad(ctx context.Context, conn grpc.ClientConnInterface, targets []*target, cfg simConfig, start time.Time) *stats {
	if cfg.open {
		return runOpen(ctx, conn, targets, cfg, start)
	}

	var (
		wg    sync.WaitGroup
		total = newStats(start)
	)
	for _, t := range targets {
		wg.Go(func() {
			st := newStats(start)
			runBot(ctx, conn, t.p, t.raidID, cfg, st)
			total.merge(st)
		})
	}
	wg.Wait()
	return total
}

// currentRate — целевой RPS в момент elapsed от старта: линейный разгон до target за ramp.
// В начале разгона возвращает минимум 1, чтобы интервал расписания не уходил в бесконечность.
func currentRate(elapsed time.Duration, target float64, ramp time.Duration) float64 {
	rate := target
	if ramp > 0 && elapsed < ramp {
		rate = target * float64(elapsed) / float64(ramp)
	}
	return max(rate, 1)
}

// runOpen — open model: планировщик выпускает запрос каждые 1/rate(t), не дожидаясь ответов.
// Если сервер не успевает, растёт число запросов в полёте (до maxInflight); сверх лимита запрос
// не отправляется и считается пропущенным — планировщик при этом не блокируется.
// Если планировщик отстал от расписания, он догоняет без пауз, а не сдвигает расписание.
func runOpen(ctx context.Context, conn grpc.ClientConnInterface, targets []*target, cfg simConfig, start time.Time) *stats {
	client := raidbossV1.NewRaidServiceClient(conn)
	var (
		wg      sync.WaitGroup
		total   = newStats(start)
		sem     = make(chan struct{}, cfg.maxInflight)
		next    = start
		nextIdx int
	)

loop:
	for ctx.Err() == nil {
		t, ok := pickTarget(targets, &nextIdx)
		if !ok {
			break // все рейды завершены
		}

		next = next.Add(time.Duration(float64(time.Second) / currentRate(time.Since(start), cfg.targetRPS, cfg.rampUp)))
		if d := time.Until(next); d > 0 {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(d):
			}
		}

		select {
		case sem <- struct{}{}:
		default:
			total.markDropped()
			continue
		}
		wg.Go(func() {
			defer func() { <-sem }()
			key := uuid.NewString()
			code := attack(ctx, client, t.p, t.raidID, key, total, cfg.budget)
			if code == codes.FailedPrecondition || code == codes.NotFound {
				t.dead.Store(true)
			}
			if code == codes.OK && rand.Float64() < cfg.dupProb { //nolint:gosec // доля дублей
				attack(ctx, client, t.p, t.raidID, key, total, cfg.budget)
			}
		})
	}
	wg.Wait()
	return total
}

// pickTarget выбирает следующего живого игрока по кругу. Нагрузка делится поровну между всеми
// игроками: на одного приходится targetRPS/len(targets), и это должно быть меньше лимита 5/с.
func pickTarget(targets []*target, idx *int) (*target, bool) {
	for range targets {
		t := targets[*idx%len(targets)]
		*idx++
		if !t.dead.Load() {
			return t, true
		}
	}
	return nil, false
}
