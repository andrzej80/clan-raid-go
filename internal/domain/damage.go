package domain

import (
	"math"
	"math/rand/v2"
)

const (
	critDamageMultiplier = 2
	critChance           = 0.1
)

func CalcDamage(power int, u float64, isCrit bool) int {
	damage := float64(power) * u
	if isCrit {
		damage *= critDamageMultiplier
	}
	return int(math.Round(damage))
}

func RollHit(power int, rng *rand.Rand) (damage int, isCrit bool) {
	u := 0.8 + 0.4*rng.Float64()
	if rng.Float64() < critChance {
		isCrit = true
	}
	return CalcDamage(power, u, isCrit), isCrit
}
