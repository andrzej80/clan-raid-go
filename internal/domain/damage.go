package domain

import (
	"encoding/binary"
	"math"
	"math/rand/v2"

	"github.com/google/uuid"
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

// RollHitForKey бросает удар детерминированно: генератор засевается idempotency_key.
// Повтор запроса с тем же ключом даёт тот же урон и тот же крит, поэтому клиент всегда видит
// именно тот удар, который будет засчитан.
func RollHitForKey(power int, key uuid.UUID) (damage int, isCrit bool) {
	hi := binary.BigEndian.Uint64(key[:8])
	lo := binary.BigEndian.Uint64(key[8:])
	return RollHit(power, rand.New(rand.NewPCG(hi, lo))) //nolint:gosec // игровая механика, не криптография
}

func RollHit(power int, rng *rand.Rand) (damage int, isCrit bool) {
	u := 0.8 + 0.4*rng.Float64()
	if rng.Float64() < critChance {
		isCrit = true
	}
	return CalcDamage(power, u, isCrit), isCrit
}
