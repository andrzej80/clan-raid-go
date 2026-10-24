package rdb

import (
	"context"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

// Board — лидерборд и кэш HP рейдов в Redis. Источник правды — Postgres: всё здесь
// восстанавливается из него (raidctl rebuild-cache).
type Board struct {
	rc redis.Cmdable
}

func NewBoard(rc redis.Cmdable) *Board {
	return &Board{rc: rc}
}

// Apply записывает состояние рейда после коммита в Postgres одним pipeline.
//
// totals — АБСОЛЮТНЫЕ суммы урона игроков (ZADD, а не ZINCRBY): повторная обработка пачки
// просто перезапишет те же значения. В канал уходит WatchRaidResponse с оставшимся HP.
func (b *Board) Apply(ctx context.Context, raidID uuid.UUID, totals []domain.PlayerDamage, hpLeft int64) error {
	payload, err := proto.Marshal(&raidbossV1.WatchRaidResponse{
		Update: &raidbossV1.WatchRaidResponse_HpLeft{HpLeft: hpLeft},
	})
	if err != nil {
		return fmt.Errorf("marshal raid update: %w", err)
	}

	pipe := b.rc.Pipeline()
	if len(totals) > 0 {
		members := make([]redis.Z, len(totals))
		for i, t := range totals {
			members[i] = redis.Z{Score: float64(t.Damage), Member: strconv.FormatInt(t.PlayerID, 10)}
		}
		pipe.ZAdd(ctx, LeaderboardKey(raidID), members...)
	}
	pipe.Set(ctx, HPKey(raidID), hpLeft, 0)
	pipe.Publish(ctx, UpdatesChannel(raidID), payload)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("apply raid state to redis: %w", err)
	}
	return nil
}

// Top возвращает до limit игроков рейда по убыванию урона. Пустой результат не отличает
// «никто не бил» от «ключа нет»: это решает вызывающий.
func (b *Board) Top(ctx context.Context, raidID uuid.UUID, limit int) ([]domain.LeaderboardEntry, error) {
	zs, err := b.rc.ZRevRangeWithScores(ctx, LeaderboardKey(raidID), 0, int64(limit)-1).Result()
	if err != nil {
		return nil, fmt.Errorf("read leaderboard: %w", err)
	}
	out := make([]domain.LeaderboardEntry, 0, len(zs))
	for _, z := range zs {
		member, ok := z.Member.(string)
		if !ok {
			return nil, fmt.Errorf("leaderboard member %v is not a string", z.Member)
		}
		id, err := strconv.ParseInt(member, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("leaderboard member %q: %w", member, err)
		}
		out = append(out, domain.LeaderboardEntry{PlayerID: id, TotalDamage: int64(z.Score)})
	}
	return out, nil
}
