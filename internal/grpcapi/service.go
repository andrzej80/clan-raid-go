// Package grpcapi — gRPC-адаптер над сервисами рейдов: переводит proto в вызовы сервисов и обратно.
//
// RaidService собирается из групп хендлеров по областям (рейды, удары, лидерборд, награды,
// стрим). У каждой группы свои зависимости, поэтому тесту и main не нужно знать про чужие:
// отсутствующая группа отвечает UNIMPLEMENTED.
package grpcapi

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

// errNotEnabled — ответ RPC, чья группа хендлеров не подключена к RaidService.
var errNotEnabled = status.Error(codes.Unimplemented, "method is not enabled")

// unimplemented встраивает заглушки сервера на глубину 2: методы групп хендлеров (глубина 1)
// затеняют их без конфликта имён.
type unimplemented struct {
	raidbossV1.UnimplementedRaidServiceServer
}

// RaidService реализует raidbossV1.RaidServiceServer, делегируя каждый RPC своей группе.
// Методы групп повышаются (promoted) в RaidService; неподключённая группа — nil, а её методы
// отвечают UNIMPLEMENTED прямо на nil-получателе.
type RaidService struct {
	unimplemented
	*RaidHandlers
	*AttackHandlers
	*LeaderboardHandlers
	*RewardsHandlers
	*WatchHandlers
}

// Option подключает группу хендлеров к RaidService.
type Option func(*RaidService)

func WithRaids(h *RaidHandlers) Option { return func(s *RaidService) { s.RaidHandlers = h } }

func WithAttacks(h *AttackHandlers) Option { return func(s *RaidService) { s.AttackHandlers = h } }

func WithLeaderboard(h *LeaderboardHandlers) Option {
	return func(s *RaidService) { s.LeaderboardHandlers = h }
}

func WithRewards(h *RewardsHandlers) Option { return func(s *RaidService) { s.RewardsHandlers = h } }

func WithWatch(h *WatchHandlers) Option { return func(s *RaidService) { s.WatchHandlers = h } }

func NewRaidService(opts ...Option) *RaidService {
	s := &RaidService{}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
