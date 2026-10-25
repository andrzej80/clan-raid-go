package domain

import (
	"slices"
	"time"

	"github.com/google/uuid"
)

// HitsOutcome — результат применения упорядоченной пачки ударов к HP босса.
type HitsOutcome struct {
	Accepted []Hit // удары, засчитанные в порядке применения (включая killing blow)
	Late     []Hit // удары после killing blow
	HPLeft   int64 // не меньше 0
	Killing  *Hit  // добивающий удар; nil, если босс выжил
}

// ApplyHits последовательно вычитает урон из hp. Удар, опустивший HP до 0 и ниже, — killing blow,
// все следующие удары опоздавшие. Чистая функция: порядок входа определяет, кто добьёт босса.
func ApplyHits(hp int64, hits []Hit) HitsOutcome {
	out := HitsOutcome{HPLeft: max(hp, 0)}
	for i, h := range hits {
		hp -= int64(h.Damage)
		out.Accepted = append(out.Accepted, h)
		if hp <= 0 {
			killing := h
			out.Killing = &killing
			out.HPLeft = 0
			out.Late = slices.Clone(hits[i+1:])
			return out
		}
		out.HPLeft = hp
	}
	return out
}

// SplitByDeadline делит удары на успевшие до окончания рейда и опоздавшие: удар, созданный
// в момент endsAt или позже, не засчитывается (правило 6 README). Порядок сохраняется.
func SplitByDeadline(hits []Hit, endsAt time.Time) (timely, late []Hit) {
	timely = make([]Hit, 0, len(hits))
	for _, h := range hits {
		if h.CreatedAt.Before(endsAt) {
			timely = append(timely, h)
		} else {
			late = append(late, h)
		}
	}
	return timely, late
}

// DedupHits убирает повторы hit_id, сохраняя порядок и первое вхождение.
func DedupHits(hits []Hit) []Hit {
	seen := make(map[uuid.UUID]struct{}, len(hits))
	out := make([]Hit, 0, len(hits))
	for _, h := range hits {
		if _, dup := seen[h.ID]; dup {
			continue
		}
		seen[h.ID] = struct{}{}
		out = append(out, h)
	}
	return out
}

// RaidHits — удары одного рейда в порядке прихода.
type RaidHits struct {
	RaidID uuid.UUID
	Hits   []Hit
}

// GroupByRaid группирует удары по рейдам. И порядок рейдов (по первому вхождению),
// и порядок ударов внутри рейда совпадают с порядком входа.
func GroupByRaid(hits []Hit) []RaidHits {
	index := make(map[uuid.UUID]int)
	var groups []RaidHits
	for _, h := range hits {
		i, ok := index[h.RaidID]
		if !ok {
			i = len(groups)
			index[h.RaidID] = i
			groups = append(groups, RaidHits{RaidID: h.RaidID})
		}
		groups[i].Hits = append(groups[i].Hits, h)
	}
	return groups
}

// PlayerDamage — суммарный урон игрока в рейде.
type PlayerDamage struct {
	PlayerID int64
	Damage   int64
	Hits     int
}

// AggregateDamage суммирует урон по игрокам; порядок — по первому появлению игрока.
func AggregateDamage(hits []Hit) []PlayerDamage {
	index := make(map[int64]int)
	var out []PlayerDamage
	for _, h := range hits {
		i, ok := index[h.PlayerID]
		if !ok {
			i = len(out)
			index[h.PlayerID] = i
			out = append(out, PlayerDamage{PlayerID: h.PlayerID})
		}
		out[i].Damage += int64(h.Damage)
		out[i].Hits++
	}
	return out
}
