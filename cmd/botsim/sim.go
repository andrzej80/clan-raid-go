package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
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
		code := attack(ctx, client, p, raidID, key, st)
		if code == codes.FailedPrecondition || code == codes.NotFound {
			return // рейд побеждён или истёк
		}
		if code == codes.OK && rand.Float64() < cfg.dupProb { //nolint:gosec // доля дублей
			attack(ctx, client, p, raidID, key, st) // повтор с тем же ключом: второго удара быть не должно
		}
	}
}

// attack отправляет удар, повторяя при UNAVAILABLE/DEADLINE_EXCEEDED/RESOURCE_EXHAUSTED с тем же
// idempotency_key. Каждая попытка попадает в статистику отдельно. Возвращает код последней попытки.
func attack(ctx context.Context, client raidbossV1.RaidServiceClient, p player, raidID, key string, st *stats) codes.Code {
	var code codes.Code
	for attempt := range maxAttempts {
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
			st.accepted++
		}
		if !retryable(code) || attempt == maxAttempts-1 {
			return code
		}
		st.retries++
		select {
		case <-ctx.Done():
			return codes.Canceled
		case <-time.After(retryBackoff << attempt):
		}
	}
	return code
}

func retryable(c codes.Code) bool {
	return c == codes.Unavailable || c == codes.DeadlineExceeded || c == codes.ResourceExhausted
}

// simulate запускает по горутине на игрока и возвращает объединённую статистику и список рейдов.
func simulate(ctx context.Context, pool *pgxpool.Pool, conn *grpc.ClientConn, cfg simConfig) (*stats, []string, error) {
	byClan, err := loadPlayers(ctx, pool, cfg.clans, cfg.players)
	if err != nil {
		return nil, nil, err
	}
	client := raidbossV1.NewRaidServiceClient(conn)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		total   = newStats()
		raidIDs []string
	)
	for _, players := range byClan {
		raidID, err := ensureRaid(ctx, pool, client, players[0], cfg)
		if err != nil {
			return nil, nil, err
		}
		raidIDs = append(raidIDs, raidID)

		for _, p := range players {
			wg.Go(func() {
				st := newStats()
				runBot(ctx, conn, p, raidID, cfg, st)
				mu.Lock()
				total.merge(st)
				mu.Unlock()
			})
		}
	}
	wg.Wait()
	return total, raidIDs, nil
}
