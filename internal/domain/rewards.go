package domain

import (
	"cmp"
	"slices"
)

var topRewards = [3]int{500, 300, 200}

const (
	generalRewardBonus = 100
	killerReward       = 250
)

func CalcRewards(playersDamage map[int]int, killerID int) []Reward {
	rewards := make([]Reward, 0, len(playersDamage)+1+len(topRewards))
	addGeneralRewards(&rewards, playersDamage)
	addTopRewards(&rewards, playersDamage)
	if _, ok := playersDamage[killerID]; !ok {
		return rewards
	}
	addKillerRewards(&rewards, killerID)
	return rewards
}

func addGeneralRewards(rewards *[]Reward, playersDamage map[int]int) {
	totalDamage := 0
	for _, playerDamage := range playersDamage {
		totalDamage += playerDamage
	}

	if totalDamage < 1 {
		return
	}

	for playerID, playerDamage := range playersDamage {
		generalReward := Reward{
			PlayerID: playerID,
			Kind:     general,
			Amount:   generalRewardBonus + (1000 * playerDamage / totalDamage),
		}
		*rewards = append(*rewards, generalReward)
	}
}

func addKillerRewards(rewards *[]Reward, killerID int) {
	killerReward := Reward{
		PlayerID: killerID,
		Kind:     killer,
		Amount:   killerReward,
	}
	*rewards = append(*rewards, killerReward)
}

func addTopRewards(rewards *[]Reward, playersDamage map[int]int) {
	topPlayers := topDamage(playersDamage, len(topRewards))
	for place, playerID := range topPlayers {
		topReward := Reward{
			PlayerID: playerID,
			Kind:     top,
			Amount:   topRewards[place],
		}
		*rewards = append(*rewards, topReward)
	}
}

func topDamage(playersDamage map[int]int, n int) []int {
	ids := make([]int, 0, len(playersDamage))
	for id := range playersDamage {
		ids = append(ids, id)
	}

	slices.SortFunc(ids, func(a, b int) int {
		// по убыванию урона
		if c := cmp.Compare(playersDamage[b], playersDamage[a]); c != 0 {
			return c
		}
		// при равном уроне — по ID, чтобы результат был стабильным
		return cmp.Compare(a, b)
	})

	return ids[:min(n, len(ids))]
}
