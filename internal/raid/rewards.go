package raid

import (
	"context"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

// maxRewardsListed ограничивает ответ ListRewards: у активного игрока наград набирается много,
// а пагинации в контракте нет.
const maxRewardsListed = 1000

// RewardsService отдаёт игроку его награды.
type RewardsService struct {
	players domain.PlayerReader
	rewards domain.RewardsReader
}

func NewRewardsService(players domain.PlayerReader, rewards domain.RewardsReader) *RewardsService {
	return &RewardsService{players: players, rewards: rewards}
}

// List возвращает награды игрока, новые первыми. Неизвестный игрок — ErrPlayerNotFound.
func (s *RewardsService) List(ctx context.Context, playerID int64) ([]domain.PlayerReward, error) {
	if _, err := s.players.GetPlayer(ctx, playerID); err != nil {
		return nil, err
	}
	return s.rewards.ListByPlayer(ctx, playerID, maxRewardsListed)
}
