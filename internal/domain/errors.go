package domain

import "errors"

var (
	ErrInvalidArgument  = errors.New("invalid argument")
	ErrRaidNotFound     = errors.New("raid not found")
	ErrClanNotFound     = errors.New("clan not found")
	ErrPlayerNotFound   = errors.New("player not found")
	ErrActiveRaidExists = errors.New("clan already has an active raid")
)
