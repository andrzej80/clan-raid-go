package domain

type RewardKind string

const (
	general RewardKind = "participation"
	top     RewardKind = "top3"
	killer  RewardKind = "killing_blow"
)

type Reward struct {
	playerID int
	kind     RewardKind
	amount   int
}
