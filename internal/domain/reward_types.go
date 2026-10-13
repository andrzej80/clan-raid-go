package domain

type RewardKind string

const (
	GeneralKind RewardKind = "participation"
	TopKind     RewardKind = "top3"
	KillerKind  RewardKind = "killing_blow"
)

type Reward struct {
	PlayerID int
	Kind     RewardKind
	Amount   int
}
