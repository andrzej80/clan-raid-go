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

// UpdateLeaderboardSize — сколько игроков уходит в стрим WatchRaid в сообщении leaderboard.
const UpdateLeaderboardSize = 10

// Apply записывает состояние рейда после коммита в Postgres одним pipeline.
//
// totals — АБСОЛЮТНЫЕ суммы урона игроков (ZADD, а не ZINCRBY): повторная обработка пачки
// просто перезапишет те же значения. В канал уходят три сообщения WatchRaidResponse: оставшееся
// HP, число принятых ударов и топ лидерборда (его нужно прочитать после ZADD, поэтому это второй обмен с Redis).
func (b *Board) Apply(ctx context.Context, raidID uuid.UUID, totals []domain.PlayerDamage, hpLeft, hitsTotal int64) error {
	hpPayload, err := proto.Marshal(&raidbossV1.WatchRaidResponse{
		Update: &raidbossV1.WatchRaidResponse_HpLeft{HpLeft: hpLeft},
	})
	if err != nil {
		return fmt.Errorf("marshal raid update: %w", err)
	}

	hitsPayload, err := proto.Marshal(&raidbossV1.WatchRaidResponse{
		Update: &raidbossV1.WatchRaidResponse_HitsTotal{HitsTotal: hitsTotal},
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
	pipe.Publish(ctx, UpdatesChannel(raidID), hpPayload)
	pipe.Publish(ctx, UpdatesChannel(raidID), hitsPayload)
	top := pipe.ZRevRangeWithScores(ctx, LeaderboardKey(raidID), 0, UpdateLeaderboardSize-1)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("apply raid state to redis: %w", err)
	}

	entries, err := entriesFromZ(top.Val())
	if err != nil {
		return err
	}
	return b.publish(ctx, raidID, &raidbossV1.WatchRaidResponse{
		Update: &raidbossV1.WatchRaidResponse_Leaderboard{Leaderboard: leaderboardToProto(entries)},
	})
}

// PublishDefeated сообщает подписчикам WatchRaid, что босс побеждён. Публикуется после коммита
// в Postgres; потерянное сообщение подписчик не увидит, но переподключение даёт актуальный snapshot.
func (b *Board) PublishDefeated(ctx context.Context, raidID uuid.UUID, killerPlayerID int64) error {
	return b.publish(ctx, raidID, finishedUpdate(raidbossV1.RaidStatus_RAID_STATUS_DEFEATED, killerPlayerID))
}

// PublishExpired сообщает подписчикам WatchRaid, что время рейда вышло.
func (b *Board) PublishExpired(ctx context.Context, raidID uuid.UUID) error {
	return b.publish(ctx, raidID, finishedUpdate(raidbossV1.RaidStatus_RAID_STATUS_EXPIRED, 0))
}

func finishedUpdate(status raidbossV1.RaidStatus, killerPlayerID int64) *raidbossV1.WatchRaidResponse {
	return &raidbossV1.WatchRaidResponse{Update: &raidbossV1.WatchRaidResponse_Finished{
		Finished: &raidbossV1.RaidFinished{Status: status, KillerPlayerId: killerPlayerID},
	}}
}

func (b *Board) publish(ctx context.Context, raidID uuid.UUID, update *raidbossV1.WatchRaidResponse) error {
	payload, err := proto.Marshal(update)
	if err != nil {
		return fmt.Errorf("marshal raid update: %w", err)
	}
	if err := b.rc.Publish(ctx, UpdatesChannel(raidID), payload).Err(); err != nil {
		return fmt.Errorf("publish raid update: %w", err)
	}
	return nil
}

func leaderboardToProto(entries []domain.LeaderboardEntry) *raidbossV1.GetLeaderboardResponse {
	out := make([]*raidbossV1.LeaderboardEntry, len(entries))
	for i, e := range entries {
		out[i] = &raidbossV1.LeaderboardEntry{PlayerId: e.PlayerID, TotalDamage: e.TotalDamage}
	}
	return &raidbossV1.GetLeaderboardResponse{Entries: out}
}

// Top возвращает до limit игроков рейда по убыванию урона. Пустой результат не отличает
// «никто не бил» от «ключа нет»: это решает вызывающий.
func (b *Board) Top(ctx context.Context, raidID uuid.UUID, limit int) ([]domain.LeaderboardEntry, error) {
	zs, err := b.rc.ZRevRangeWithScores(ctx, LeaderboardKey(raidID), 0, int64(limit)-1).Result()
	if err != nil {
		return nil, fmt.Errorf("read leaderboard: %w", err)
	}
	return entriesFromZ(zs)
}

func entriesFromZ(zs []redis.Z) ([]domain.LeaderboardEntry, error) {
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
