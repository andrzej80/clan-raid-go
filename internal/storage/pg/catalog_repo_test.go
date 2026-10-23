package pg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

func createPlayer(t *testing.T, pool *pgxpool.Pool, clanID int64, power int) (int64, string) {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(),
		`INSERT INTO players (clan_id, name, power) VALUES ($1,$2,$3) RETURNING id`, clanID, t.Name(), power).Scan(&id)
	if err != nil {
		t.Fatalf("create player: %v", err)
	}
	return id, t.Name()
}

func newPlayer(playerID, clanID int64, power int, name string) *domain.Player {
	return &domain.Player{
		ID:     playerID,
		ClanID: clanID,
		Name:   name,
		Power:  power,
	}
}

func TestCatalogRepo_CreateClansKeepsOrder(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	repo := pg.NewCatalogRepo(pool)
	ctx := context.Background()

	names := []string{t.Name() + "-a", t.Name() + "-b", t.Name() + "-c"}
	ids, err := repo.CreateClans(ctx, names)
	if err != nil {
		t.Fatalf("CreateClans: %v", err)
	}
	if len(ids) != len(names) {
		t.Fatalf("got %d ids, want %d", len(ids), len(names))
	}

	for i, id := range ids {
		var name string
		if err := pool.QueryRow(ctx, `SELECT name FROM clans WHERE id = $1`, id).Scan(&name); err != nil {
			t.Fatalf("select clan %d: %v", id, err)
		}
		if name != names[i] {
			t.Fatalf("id %d has name %q, want %q", id, name, names[i])
		}
	}
}

func TestCatalogRepo_CreatePlayers(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	repo := pg.NewCatalogRepo(pool)
	ctx := context.Background()
	clanID := createClan(t, pool)

	players := []domain.Player{
		{ClanID: clanID, Name: "p1", Power: domain.MinPlayerPower},
		{ClanID: clanID, Name: "p2", Power: 500},
		{ClanID: clanID, Name: "p3", Power: domain.MaxPlayerPower},
	}
	if err := repo.CreatePlayers(ctx, players); err != nil {
		t.Fatalf("CreatePlayers: %v", err)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM players WHERE clan_id = $1`, clanID).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != len(players) {
		t.Fatalf("stored %d players, want %d", n, len(players))
	}
}

func TestCatalogRepo_CreatePlayersRejectsBadPower(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	repo := pg.NewCatalogRepo(pool)
	clanID := createClan(t, pool)

	err := repo.CreatePlayers(context.Background(), []domain.Player{
		{ClanID: clanID, Name: "weak", Power: domain.MinPlayerPower - 1},
	})
	if err == nil {
		t.Fatal("power below minimum must be rejected by CHECK constraint")
	}
}

// Сид идёт одной транзакцией: ошибка на игроках откатывает и кланы.
func TestCatalogRepo_RollbackWithTx(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	repo := pg.NewCatalogRepo(pool)
	txm := pg.NewTxManager(pool)
	ctx := context.Background()
	clanName := t.Name()

	err := txm.WithinTx(ctx, func(ctx context.Context) error {
		ids, err := repo.CreateClans(ctx, []string{clanName})
		if err != nil {
			return err
		}
		return repo.CreatePlayers(ctx, []domain.Player{{ClanID: ids[0], Name: "weak", Power: 1}})
	})
	if err == nil {
		t.Fatal("expected error from invalid player")
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM clans WHERE name = $1`, clanName).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("clan must be rolled back, found %d", n)
	}
}

func TestCatalogRepo_GetPlayer(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	repo := pg.NewCatalogRepo(pool)
	ctx := context.Background()
	clanId := createClan(t, pool)
	power := 300
	playerId, name := createPlayer(t, pool, clanId, power)
	want := newPlayer(playerId, clanId, power, name)

	got, err := repo.GetPlayer(ctx, want.ID)
	if err != nil {
		t.Fatalf("GetPlayer: %v", err)
	}
	if got.ID != want.ID || got.ClanID != want.ClanID || got.Name != want.Name || got.Power != want.Power {
		t.Fatalf("player mismatch:\n got  %+v\n want %+v", got, want)
	}
}

func TestCatalogRepo_GetPlayerNotFound(t *testing.T) {
	t.Parallel()

	repo := pg.NewCatalogRepo(requirePool(t))

	_, err := repo.GetPlayer(context.Background(), 999999999)
	if !errors.Is(err, domain.ErrPlayerNotFound) {
		t.Fatalf("err = %v, want ErrPlayerNotFound", err)
	}
}
