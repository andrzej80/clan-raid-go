package domain

import "errors"

var (
	ErrInvalidArgument  = errors.New("invalid argument")
	ErrRaidNotFound     = errors.New("raid not found")
	ErrClanNotFound     = errors.New("clan not found")
	ErrPlayerNotFound   = errors.New("player not found")
	ErrActiveRaidExists = errors.New("clan already has an active raid")
	ErrWrongClan        = errors.New("raid belongs to another clan")
	ErrRaidNotActive    = errors.New("raid is not active")
	// ErrQueueUnavailable — удар не удалось отправить в очередь; клиент повторяет с тем же ключом.
	ErrQueueUnavailable = errors.New("hit queue unavailable")
)
