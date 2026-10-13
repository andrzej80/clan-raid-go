package domain

type RewardKind string

const (
	general RewardKind = "participation"
	top     RewardKind = "top3"
	killer  RewardKind = "killing_blow"
)

type Reward struct {
	PlayerID int
	Kind     RewardKind
	Amount   int
}
