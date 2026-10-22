package domain

import "context"

const (
	MinPlayerPower = 100
	MaxPlayerPower = 1000
)

type Player struct {
	ID     int64
	ClanID int64
	Name   string
	Power  int
}

// CatalogRepository — справочные данные: кланы и игроки.
type CatalogRepository interface {
	// CreateClans создаёт кланы с заданными именами и возвращает их id в том же порядке.
	CreateClans(ctx context.Context, names []string) ([]int64, error)
	// CreatePlayers создаёт игроков пачкой; ID в переданных значениях игнорируется.
	CreatePlayers(ctx context.Context, players []Player) error
}
