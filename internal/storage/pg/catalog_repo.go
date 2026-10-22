package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type CatalogRepo struct {
	pool *pgxpool.Pool
}

func NewCatalogRepo(pool *pgxpool.Pool) *CatalogRepo {
	return &CatalogRepo{pool: pool}
}

var (
	_ domain.CatalogRepository = (*CatalogRepo)(nil)
	_ domain.PlayerReader      = (*CatalogRepo)(nil)
)

const playerColumns = `id, clan_id, name, power`

func (r *CatalogRepo) CreateClans(ctx context.Context, names []string) ([]int64, error) {
	// unnest ... WITH ORDINALITY сохраняет порядок: RETURNING идёт в порядке вставки строк.
	rows, err := conn(ctx, r.pool).Query(ctx, `
		INSERT INTO clans (name)
		SELECT name FROM unnest($1::text[]) WITH ORDINALITY AS t(name, ord) ORDER BY ord
		RETURNING id`, names)
	if err != nil {
		return nil, fmt.Errorf("insert clans: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("insert clans: %w", err)
	}
	return ids, nil
}

func (r *CatalogRepo) CreatePlayers(ctx context.Context, players []domain.Player) error {
	if len(players) == 0 {
		return nil
	}
	// CopyFrom не входит в dbtx, поэтому транзакцию из контекста подхватываем вручную.
	rows := pgx.CopyFromSlice(len(players), func(i int) ([]any, error) {
		p := players[i]
		return []any{p.ClanID, p.Name, p.Power}, nil
	})
	columns := []string{"clan_id", "name", "power"}

	var err error
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"players"}, columns, rows)
	} else {
		_, err = r.pool.CopyFrom(ctx, pgx.Identifier{"players"}, columns, rows)
	}
	if err != nil {
		return fmt.Errorf("copy players: %w", err)
	}
	return nil
}

func (r *CatalogRepo) GetPlayer(ctx context.Context, id int64) (*domain.Player, error) {
	var player domain.Player
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT `+playerColumns+` FROM players WHERE id = $1`, id,
	).Scan(
		&player.ID, &player.ClanID, &player.Name, &player.Power,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrPlayerNotFound
		}
		return nil, fmt.Errorf("get player: %w", err)
	}
	return &player, nil
}
