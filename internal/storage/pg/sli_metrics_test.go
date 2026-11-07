package pg_test

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/raid"
	"github.com/andrzej80/clan-raid-go/internal/rewards"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

// histogram возвращает число и сумму наблюдений гистограммы name.
func histogram(t *testing.T, reader *sdkmetric.ManualReader, name string) (count uint64, sum float64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if h, ok := m.Data.(metricdata.Histogram[float64]); ok && m.Name == name {
				for _, dp := range h.DataPoints {
					count += dp.Count
					sum += dp.Sum
				}
			}
		}
	}
	return count, sum
}

func newMeter(t *testing.T) (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return mp, reader
}

// Свежесть удара (SLI «удар виден в игре») — от created_at до коммита, по наблюдению на каждый
// принятый удар; опоздавшие и дубли не считаются.
func TestHitFreshnessMetric(t *testing.T) {
	t.Parallel()

	mp, reader := newMeter(t)
	created := time.Now().UTC().Truncate(time.Microsecond)
	clock := func() time.Time { return created.Add(3 * time.Second) }
	env := newProcessorEnv(t, 1, raid.WithProcessorMeterProvider(mp), raid.WithProcessorClock(clock))
	r := env.newRaidWithHP(t, 1000)

	batch := []domain.Hit{env.hit(r.ID, env.players[0], 10), env.hit(r.ID, env.players[0], 20)}
	for i := range batch {
		batch[i].CreatedAt = created
	}
	mustProcess(t, env.processor, batch)
	mustProcess(t, env.processor, batch) // повтор: дубли свежесть не пишут

	count, sum := histogram(t, reader, "raidboss.consumer.hit.freshness")
	if count != 2 || sum < 5.9 || sum > 6.1 {
		t.Fatalf("freshness: count %d, sum %.2f; want 2 hits of 3 s", count, sum)
	}
}

// Задержка наград (SLI «награды выданы») — от finished_at рейда до записи наград, один раз на рейд.
func TestRewardsDelayMetric(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 2)
	r := defeatedRaid(t, env)
	mp, reader := newMeter(t)
	p, err := rewards.NewProcessor(pg.NewTxManager(env.pool), pg.NewRewardsRepo(env.pool), pg.NewOutboxRepo(env.pool),
		rewardsTopic, rewards.WithMeterProvider(mp))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	ev := []domain.RaidEvent{{RaidID: r.ID, Kind: domain.RaidEventDefeated}}
	for range 2 { // повторная доставка события задержку не пишет
		if _, err := p.ProcessEvents(context.Background(), ev); err != nil {
			t.Fatalf("ProcessEvents: %v", err)
		}
	}

	count, sum := histogram(t, reader, "raidboss.rewards.delay")
	if count != 1 || sum < 0 || sum > 30 {
		t.Fatalf("rewards delay: count %d, sum %.2f s; want one observation within seconds", count, sum)
	}
}
