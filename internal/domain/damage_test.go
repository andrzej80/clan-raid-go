package domain

import (
	"math"
	"math/rand/v2"
	"testing"
)

func TestCalcDamage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		power        int
		u            float64
		isCrit       bool
		resultDamage int
	}{
		{name: "check 500 0.8 regular", power: 500, u: 0.8, isCrit: false, resultDamage: 400},
		{name: "check 400 1.0 regular", power: 400, u: 1.0, isCrit: false, resultDamage: 400},
		{name: "check 500 1.0 crit", power: 500, u: 1.0, isCrit: true, resultDamage: 1000},
		{name: "check 100 1.2 crit", power: 100, u: 1.2, isCrit: true, resultDamage: 240},
		{name: "check 105 1.1 crit", power: 105, u: 1.1, isCrit: true, resultDamage: 231},
		{name: "check 405 1.1 regular", power: 405, u: 1.1, isCrit: false, resultDamage: 446},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calculatedDamage := CalcDamage(tc.power, tc.u, tc.isCrit)
			if calculatedDamage != tc.resultDamage {
				t.Fatalf("CalcDamage() wrog calculation mismatch got = %v, expected %v", calculatedDamage, tc.resultDamage)
			}
		})
	}
}

func TestRollHit_Determinism(t *testing.T) {
	t.Parallel()
	const (
		power = 100
		seed  = 42
		steps = 100
	)

	results1 := make([]int, steps)
	crits1 := make([]bool, steps)
	pcg := rand.NewPCG(seed, 0)
	r := rand.New(pcg)
	for i := range steps {
		results1[i], crits1[i] = RollHit(power, r)
	}

	pcg = rand.NewPCG(seed, 0)
	r = rand.New(pcg)
	for i := range steps {
		dmg, isCrit := RollHit(power, r)
		if dmg != results1[i] || isCrit != crits1[i] {
			t.Fatalf("Разрушена детерминированность на шаге %d: получили (%d, %t), ожидали (%d, %t)",
				i, dmg, isCrit, results1[i], crits1[i])
		}
	}
}

func TestRollHit_Statistics(t *testing.T) {
	t.Parallel()
	const (
		power     = 100
		seed      = 123456
		totalHits = 100_000
		tolerance = 100 // 1%
	)

	expectedAverage := int(math.Round(1.1 * power))
	expectedCritRate := 0.10

	var totalDamage int
	var critCount int
	pcg := rand.NewPCG(seed, 0)
	r := rand.New(pcg)
	for range totalHits {
		dmg, isCrit := RollHit(power, r)

		if isCrit {
			critCount++
			minCrit, maxCrit := int(math.Round(1.6*power)), int(math.Round(2.4*power))
			if dmg < minCrit || dmg > maxCrit {
				t.Fatalf("Крит вышел за пределы [%d, %d]: %d", minCrit, maxCrit, dmg)
			}
		} else {
			minNormal, maxNormal := int(math.Round(0.8*power)), int(math.Round(1.2*power))
			if dmg < minNormal || dmg > maxNormal {
				t.Fatalf("Обычный удар вышел за пределы [%d, %d]: %d", minNormal, maxNormal, dmg)
			}
		}

		totalDamage += dmg
	}

	actualAverage := totalDamage / totalHits
	diff := max(actualAverage, expectedAverage) - min(actualAverage, expectedAverage)

	if diff > expectedAverage/tolerance {
		t.Errorf("Средний урон %d отклоняется от ожидаемого %d более чем на %d%% (разница: %.d%%)",
			actualAverage, expectedAverage, tolerance/100, diff)
	}

	actualCritRate := float64(critCount) / float64(totalHits)
	diffCrit := math.Abs(actualCritRate - expectedCritRate)

	if diffCrit > tolerance {
		t.Errorf("Доля критов %.2f%% отклоняется от ожидаемых %.2f%% (разница: %.2f%%)",
			actualCritRate*100, expectedCritRate*100, diffCrit*100)
	}
}
