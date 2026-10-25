package verify

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

const rewardsInvariant = "rewards of a defeated raid match the damage distribution"

// RunRewards сверяет выданные награды с пересчётом по raid_damage и killer_player_id убитых рейдов
// (та же чистая функция, что у rewards-worker).
//
// Награды рейда вставляются одной транзакцией, поэтому у убитого рейда их либо нет (worker ещё не
// успел), либо все. Рейды без наград проверка пропускает, если requireAll выключен; включать его
// нужно после того, как rewards-worker разобрал очередь, иначе отсутствие наград — не ошибка.
func RunRewards(ctx context.Context, db Querier, raidIDs []uuid.UUID, requireAll bool) ([]Violation, error) {
	var filter any
	if raidIDs != nil {
		filter = raidIDs
	}

	raids, err := defeatedRaids(ctx, db, filter)
	if err != nil {
		return nil, err
	}
	damage, err := damageByRaid(ctx, db, filter)
	if err != nil {
		return nil, err
	}
	granted, err := rewardsByRaid(ctx, db, filter)
	if err != nil {
		return nil, err
	}

	var violations []Violation
	for _, raidID := range sortedRaidIDs(raids) {
		killer := raids[raidID]
		have := granted[raidID]
		if len(have) == 0 {
			if requireAll {
				violations = append(violations, Violation{
					Invariant: 0, Name: "defeated raid has rewards (after rewards-worker drained the queue)",
					Subject: raidID.String(), Detail: "no rewards",
				})
			}
			continue
		}
		want := domain.CalcRewards(damage[raidID], killer)
		if diff := diffRewards(want, have); diff != "" {
			violations = append(violations, Violation{
				Invariant: 5, Name: rewardsInvariant, Subject: raidID.String(), Detail: diff,
			})
		}
	}
	return violations, nil
}

type rewardKey struct {
	player int
	kind   domain.RewardKind
}

func defeatedRaids(ctx context.Context, db Querier, filter any) (map[uuid.UUID]int, error) {
	rows, err := db.Query(ctx, `
		SELECT id, COALESCE(killer_player_id, 0)
		FROM raids
		WHERE status = 'defeated' AND ($1::uuid[] IS NULL OR id = ANY($1))`, filter)
	if err != nil {
		return nil, fmt.Errorf("select defeated raids: %w", err)
	}
	defer rows.Close()

	out := map[uuid.UUID]int{}
	for rows.Next() {
		var (
			id     uuid.UUID
			killer int64
		)
		if err := rows.Scan(&id, &killer); err != nil {
			return nil, fmt.Errorf("scan defeated raid: %w", err)
		}
		out[id] = int(killer)
	}
	return out, rows.Err()
}

func damageByRaid(ctx context.Context, db Querier, filter any) (map[uuid.UUID]map[int]int, error) {
	rows, err := db.Query(ctx, `
		SELECT d.raid_id, d.player_id, d.total_damage
		FROM raid_damage d
		JOIN raids r ON r.id = d.raid_id
		WHERE r.status = 'defeated' AND ($1::uuid[] IS NULL OR r.id = ANY($1))`, filter)
	if err != nil {
		return nil, fmt.Errorf("select damage: %w", err)
	}
	defer rows.Close()

	out := map[uuid.UUID]map[int]int{}
	for rows.Next() {
		var (
			raidID         uuid.UUID
			player, damage int64
		)
		if err := rows.Scan(&raidID, &player, &damage); err != nil {
			return nil, fmt.Errorf("scan damage: %w", err)
		}
		if out[raidID] == nil {
			out[raidID] = map[int]int{}
		}
		out[raidID][int(player)] = int(damage)
	}
	return out, rows.Err()
}

func rewardsByRaid(ctx context.Context, db Querier, filter any) (map[uuid.UUID]map[rewardKey]int, error) {
	rows, err := db.Query(ctx, `
		SELECT w.raid_id, w.player_id, w.kind, w.amount
		FROM rewards w
		JOIN raids r ON r.id = w.raid_id
		WHERE r.status = 'defeated' AND ($1::uuid[] IS NULL OR r.id = ANY($1))`, filter)
	if err != nil {
		return nil, fmt.Errorf("select rewards: %w", err)
	}
	defer rows.Close()

	out := map[uuid.UUID]map[rewardKey]int{}
	for rows.Next() {
		var (
			raidID uuid.UUID
			player int64
			kind   string
			amount int
		)
		if err := rows.Scan(&raidID, &player, &kind, &amount); err != nil {
			return nil, fmt.Errorf("scan reward: %w", err)
		}
		if out[raidID] == nil {
			out[raidID] = map[rewardKey]int{}
		}
		out[raidID][rewardKey{int(player), domain.RewardKind(kind)}] = amount
	}
	return out, rows.Err()
}

// diffRewards сравнивает ожидаемые награды с выданными; пустая строка — совпадают.
func diffRewards(want []domain.Reward, have map[rewardKey]int) string {
	expected := make(map[rewardKey]int, len(want))
	for _, r := range want {
		expected[rewardKey{r.PlayerID, r.Kind}] = r.Amount
	}

	var problems []string
	for k, amount := range expected {
		switch got, ok := have[k]; {
		case !ok:
			problems = append(problems, fmt.Sprintf("missing %s for player %d (want %d)", k.kind, k.player, amount))
		case got != amount:
			problems = append(problems,
				fmt.Sprintf("%s for player %d is %d, want %d", k.kind, k.player, got, amount))
		}
	}
	for k, amount := range have {
		if _, ok := expected[k]; !ok {
			problems = append(problems, fmt.Sprintf("unexpected %s for player %d (%d)", k.kind, k.player, amount))
		}
	}
	slices.Sort(problems)
	if len(problems) > 3 {
		problems = append(problems[:3], fmt.Sprintf("and %d more", len(problems)-3))
	}
	return strings.Join(problems, "; ")
}

func sortedRaidIDs(m map[uuid.UUID]int) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) })
	return ids
}
