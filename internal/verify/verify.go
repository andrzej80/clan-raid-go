// Package verify проверяет инварианты из раздела 7 README по данным PostgreSQL.
package verify

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Querier — то, что нужно проверкам от *pgxpool.Pool.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Violation — нарушение инварианта.
type Violation struct {
	Invariant int    // номер из README; 0 — дополнительные проверки
	Name      string // короткое описание инварианта
	Subject   string // идентификатор нарушителя (рейд, клан)
	Detail    string
}

func (v Violation) String() string {
	return fmt.Sprintf("invariant %d (%s): %s: %s", v.Invariant, v.Name, v.Subject, v.Detail)
}

type check struct {
	invariant int
	name      string
	// sql возвращает строки (subject, detail) нарушителей; $1 — фильтр рейдов (NULL = все).
	sql string
}

// Каждый запрос возвращает только нарушителей: пустой результат — инвариант выполнен.
var checks = []check{
	{1, "defeated raid: damage covers hp_max, and the killing blow was needed", `
		SELECT r.id::text,
		       format('sum(damage)=%s, killer damage=%s, hp_max=%s', COALESCE(s.total, 0), COALESCE(k.damage, 0), r.hp_max)
		FROM raids r
		LEFT JOIN (SELECT raid_id, sum(damage) AS total FROM raid_hits GROUP BY raid_id) s ON s.raid_id = r.id
		LEFT JOIN raid_hits k ON k.hit_id = r.killer_hit_id
		WHERE r.status = 'defeated'
		  AND ($1::uuid[] IS NULL OR r.id = ANY($1))
		  AND (COALESCE(s.total, 0) < r.hp_max OR COALESCE(s.total, 0) - COALESCE(k.damage, 0) >= r.hp_max)
		LIMIT ` + limit},

	{2, "defeated raid has exactly one killer hit that exists in raid_hits", `
		SELECT r.id::text,
		       format('killer_hit_id=%s killer_player_id=%s', COALESCE(r.killer_hit_id::text, 'NULL'),
		              COALESCE(r.killer_player_id::text, 'NULL'))
		FROM raids r
		LEFT JOIN raid_hits k ON k.hit_id = r.killer_hit_id
		WHERE r.status = 'defeated'
		  AND ($1::uuid[] IS NULL OR r.id = ANY($1))
		  AND (k.hit_id IS NULL OR k.raid_id <> r.id OR k.player_id IS DISTINCT FROM r.killer_player_id)
		LIMIT ` + limit},

	{3, "no accepted hits after the killing blow", `
		SELECT r.id::text, format('killer seq=%s, last hit seq=%s', k.seq, l.last_seq)
		FROM raids r
		JOIN raid_hits k ON k.hit_id = r.killer_hit_id
		JOIN (SELECT raid_id, max(seq) AS last_seq FROM raid_hits GROUP BY raid_id) l ON l.raid_id = r.id
		WHERE r.status = 'defeated'
		  AND ($1::uuid[] IS NULL OR r.id = ANY($1))
		  AND k.seq < l.last_seq
		LIMIT ` + limit},

	{4, "raid_damage equals the aggregate of raid_hits", `
		SELECT COALESCE(d.raid_id, a.raid_id)::text || '/player ' || COALESCE(d.player_id, a.player_id)::text,
		       format('raid_damage: total=%s hits=%s; raid_hits: total=%s hits=%s',
		              COALESCE(d.total_damage::text, 'none'), COALESCE(d.hits::text, 'none'),
		              COALESCE(a.total::text, 'none'), COALESCE(a.hits::text, 'none'))
		FROM raid_damage d
		FULL JOIN (SELECT raid_id, player_id, sum(damage) AS total, count(*) AS hits
		           FROM raid_hits GROUP BY raid_id, player_id) a
		       ON a.raid_id = d.raid_id AND a.player_id = d.player_id
		WHERE ($1::uuid[] IS NULL OR COALESCE(d.raid_id, a.raid_id) = ANY($1))
		  AND (d.raid_id IS NULL OR a.raid_id IS NULL OR d.total_damage <> a.total OR d.hits <> a.hits)
		LIMIT ` + limit},

	{5, "rewards exist only for defeated raids", `
		SELECT rw.raid_id::text, format('%s reward(s) for a raid with status %s', count(*), r.status)
		FROM rewards rw
		JOIN raids r ON r.id = rw.raid_id
		WHERE r.status <> 'defeated'
		  AND ($1::uuid[] IS NULL OR rw.raid_id = ANY($1))
		GROUP BY rw.raid_id, r.status
		LIMIT ` + limit},

	{5, "at most one reward per (raid, player, kind)", `
		SELECT raid_id::text || '/player ' || player_id::text, format('%s rewards of kind %s', count(*), kind)
		FROM rewards
		WHERE ($1::uuid[] IS NULL OR raid_id = ANY($1))
		GROUP BY raid_id, player_id, kind
		HAVING count(*) > 1
		LIMIT ` + limit},

	{7, "at most one active raid per clan", `
		SELECT clan_id::text, format('%s active raids', count(*))
		FROM raids
		WHERE status = 'active'
		  AND ($1::uuid[] IS NULL OR clan_id IN (SELECT clan_id FROM raids WHERE id = ANY($1)))
		GROUP BY clan_id
		HAVING count(*) > 1
		LIMIT ` + limit},

	{0, "hp_left = max(hp_max - sum(damage), 0)", `
		SELECT r.id::text, format('hp_left=%s, expected=%s', r.hp_left, GREATEST(r.hp_max - COALESCE(s.total, 0), 0))
		FROM raids r
		LEFT JOIN (SELECT raid_id, sum(damage) AS total FROM raid_hits GROUP BY raid_id) s ON s.raid_id = r.id
		WHERE ($1::uuid[] IS NULL OR r.id = ANY($1))
		  AND r.hp_left <> GREATEST(r.hp_max - COALESCE(s.total, 0), 0)
		LIMIT ` + limit},
}

// limit — максимум примеров нарушителей на один инвариант в отчёте (подставляется в SQL).
const limit = "20"

// Run выполняет все проверки. raidIDs ограничивает проверку указанными рейдами; nil — все рейды.
func Run(ctx context.Context, db Querier, raidIDs []uuid.UUID) ([]Violation, error) {
	var filter any
	if raidIDs != nil {
		filter = raidIDs
	}

	var violations []Violation
	for _, c := range checks {
		found, err := runCheck(ctx, db, c, filter)
		if err != nil {
			return nil, fmt.Errorf("invariant %d (%s): %w", c.invariant, c.name, err)
		}
		violations = append(violations, found...)
	}
	return violations, nil
}

func runCheck(ctx context.Context, db Querier, c check, filter any) ([]Violation, error) {
	rows, err := db.Query(ctx, c.sql, filter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Violation
	for rows.Next() {
		v := Violation{Invariant: c.invariant, Name: c.name}
		if err := rows.Scan(&v.Subject, &v.Detail); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
