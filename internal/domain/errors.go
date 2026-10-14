package domain

import "errors"

var (
	ErrRaidNotFound     = errors.New("raid not found")
	ErrClanNotFound     = errors.New("clan not found")
	ErrActiveRaidExists = errors.New("clan already has an active raid")
)
