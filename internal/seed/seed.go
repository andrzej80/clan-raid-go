// Package seed наполняет БД тестовыми кланами и игроками.
package seed

import (
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type Options struct {
	Clans          int
	PlayersPerClan int
}

type Result struct {
	Clans   int
	Players int
}

// Run создаёт кланы и игроков одной транзакцией: либо всё, либо ничего.
// Повторный запуск добавляет новые данные, а не заменяет старые.
func Run(
	ctx context.Context,
	repo domain.CatalogRepository,
	tx domain.TxManager,
	opts Options,
	rng *rand.Rand,
) (Result, error) {
	if opts.Clans <= 0 {
		return Result{}, fmt.Errorf("%w: clans must be positive", domain.ErrInvalidArgument)
	}
	if opts.PlayersPerClan <= 0 {
		return Result{}, fmt.Errorf("%w: players per clan must be positive", domain.ErrInvalidArgument)
	}

	var res Result
	err := tx.WithinTx(ctx, func(ctx context.Context) error {
		clanIDs, err := repo.CreateClans(ctx, clanNames(opts.Clans))
		if err != nil {
			return err
		}

		players := make([]domain.Player, 0, len(clanIDs)*opts.PlayersPerClan)
		for i, clanID := range clanIDs {
			for j := range opts.PlayersPerClan {
				players = append(players, domain.Player{
					ClanID: clanID,
					Name:   fmt.Sprintf("player-%d-%d", i+1, j+1),
					Power:  randomPower(rng),
				})
			}
		}
		if err := repo.CreatePlayers(ctx, players); err != nil {
			return err
		}

		res = Result{Clans: len(clanIDs), Players: len(players)}
		return nil
	})
	return res, err
}

func clanNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("clan-%d", i+1)
	}
	return names
}

func randomPower(rng *rand.Rand) int {
	return domain.MinPlayerPower + rng.IntN(domain.MaxPlayerPower-domain.MinPlayerPower+1)
}
