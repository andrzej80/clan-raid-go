package watch

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
)

// Бенчмарки сравнивают ActorHub и MutexHub:
//
//	go test ./internal/watch -run '^$' -bench . -benchtime 100000x -cpu 1,4,12 -count 3
//
// В проде у каждого рейда своя горутина-источник (Redis), поэтому публикаторов здесь
// столько же, сколько рейдов (но не больше GOMAXPROCS), и каждый владеет своими рейдами.

const (
	benchBuffer = 4096 // с запасом над окном, чтобы подписчик не был отключён как медленный
	benchWindow = 256  // сколько обновлений рейда может быть «в полёте» до публикации следующих
)

type benchRoom struct {
	id        uuid.UUID
	sink      func(*Update)
	published atomic.Int64
	delivered atomic.Int64 // суммарно по всем подписчикам рейда
}

// BenchmarkFanout измеряет пропускную способность раздачи: b.N обновлений в рейды, каждое
// уходит всем подписчикам рейда. Время включает ожидание, пока все подписчики всё прочитают.
func BenchmarkFanout(b *testing.B) {
	shapes := []struct{ rooms, subs int }{
		{1, 1000}, // один популярный рейд
		{100, 10}, // типичная нагрузка
		{1000, 1}, // много рейдов с одним зрителем
	}
	for _, impl := range hubImpls {
		for _, sh := range shapes {
			b.Run(fmt.Sprintf("%s/rooms=%d/subs=%d", impl.name, sh.rooms, sh.subs), func(b *testing.B) {
				benchFanout(b, impl.make, sh.rooms, sh.subs)
			})
		}
	}
}

// fanoutBench — подписчики и счётчики одного прогона BenchmarkFanout.
type fanoutBench struct {
	rooms     []*benchRoom
	subsCount int64
	dropped   atomic.Bool
	consumers sync.WaitGroup
}

// subscribeAll подписывает subsPerRoom читателей на каждый рейд; читатель считает доставленное.
func (fb *fanoutBench) subscribeAll(
	ctx context.Context, b *testing.B, hub Hub, feeder *fakeFeeder, nRooms, subsPerRoom int,
) {
	b.Helper()
	fb.rooms = make([]*benchRoom, nRooms)
	for i := range fb.rooms {
		r := &benchRoom{id: uuid.New()}
		for range subsPerRoom {
			sub, err := hub.Subscribe(ctx, r.id)
			if err != nil {
				b.Fatalf("Subscribe: %v", err)
			}
			fb.consumers.Go(func() { fb.consume(ctx, sub, r) })
		}
		r.sink = feeder.sink(r.id)
		fb.rooms[i] = r
	}
}

func (fb *fanoutBench) consume(ctx context.Context, sub *Subscription, r *benchRoom) {
	defer sub.Close()
	for {
		if _, err := sub.Next(ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				fb.dropped.Store(true)
			}
			return
		}
		r.delivered.Add(1)
	}
}

// publish отдаёт n обновлений рейдам, распределяя рейды между публикаторами.
func (fb *fanoutBench) publish(n, publishers int) {
	update := hpUpdate(1)
	var pubs sync.WaitGroup
	for g := range publishers {
		pubs.Go(func() {
			var mine []*benchRoom
			for i := g; i < len(fb.rooms); i += publishers {
				mine = append(mine, fb.rooms[i])
			}
			for i := g; i < n; i += publishers {
				r := mine[(i/publishers)%len(mine)]
				// Окно: не публикуем дальше, чем читатели успевают, иначе нас отключат как медленных.
				for r.published.Load()-r.delivered.Load()/fb.subsCount >= benchWindow {
					runtime.Gosched()
				}
				r.published.Add(1)
				r.sink(update)
			}
		})
	}
	pubs.Wait()
}

// waitDelivered ждёт, пока все читатели получат всё опубликованное.
func (fb *fanoutBench) waitDelivered() {
	for _, r := range fb.rooms {
		for r.delivered.Load() < r.published.Load()*fb.subsCount && !fb.dropped.Load() {
			runtime.Gosched()
		}
	}
}

func benchFanout(b *testing.B, mk hubFactory, nRooms, subsPerRoom int) {
	feeder := newFakeFeeder()
	hub := mk(feeder, Config{BufferSize: benchBuffer, Interval: -1})
	ctx, cancel := context.WithCancel(b.Context())

	fb := &fanoutBench{subsCount: int64(subsPerRoom)}
	fb.subscribeAll(ctx, b, hub, feeder, nRooms, subsPerRoom)

	b.ResetTimer()
	fb.publish(b.N, min(nRooms, runtime.GOMAXPROCS(0)))
	fb.waitDelivered()
	b.StopTimer()

	if fb.dropped.Load() {
		b.Error("a subscriber was dropped: benchmark result is invalid")
	}
	b.ReportMetric(float64(b.N)*float64(subsPerRoom)/b.Elapsed().Seconds(), "deliveries/s")

	cancel()
	fb.consumers.Wait()
	hub.Close()
}

// BenchmarkSubscribeChurn измеряет подписку и отписку при параллельном потоке обновлений:
// raids=1 — шторм переподключений к одному рейду после рестарта сервера.
func BenchmarkSubscribeChurn(b *testing.B) {
	for _, impl := range hubImpls {
		for _, raids := range []int{1, 100} {
			b.Run(fmt.Sprintf("%s/raids=%d", impl.name, raids), func(b *testing.B) {
				benchChurn(b, impl.make, raids)
			})
		}
	}
}

// startBackground запускает постоянного зрителя на каждом рейде (он держит рейд живым, чтобы
// подписка не создавала источник заново на каждой итерации) и один публикатор, который обходит
// рейды по кругу: сотня busy-loop горутин исказила бы замер.
func startBackground(ctx context.Context, b *testing.B, hub Hub, feeder *fakeFeeder, ids []uuid.UUID) *sync.WaitGroup {
	b.Helper()
	var bg sync.WaitGroup
	sinks := make([]func(*Update), 0, len(ids))
	for _, id := range ids {
		sub, err := hub.Subscribe(ctx, id)
		if err != nil {
			b.Fatalf("Subscribe: %v", err)
		}
		bg.Go(func() {
			defer sub.Close()
			for {
				if _, err := sub.Next(ctx); err != nil {
					return
				}
			}
		})
		sinks = append(sinks, feeder.sink(id))
	}
	bg.Go(func() {
		update := hpUpdate(1)
		for ctx.Err() == nil {
			for _, sink := range sinks {
				sink(update)
			}
			runtime.Gosched()
		}
	})
	return &bg
}

func benchChurn(b *testing.B, mk hubFactory, nRaids int) {
	feeder := newFakeFeeder()
	hub := mk(feeder, Config{BufferSize: benchBuffer, Interval: -1})

	ids := make([]uuid.UUID, nRaids)
	for i := range ids {
		ids[i] = uuid.New()
	}
	ctx, cancel := context.WithCancel(b.Context())
	bg := startBackground(ctx, b, hub, feeder, ids)

	var next atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sub, err := hub.Subscribe(ctx, ids[int(next.Add(1))%nRaids])
			if err != nil {
				b.Errorf("Subscribe: %v", err)
				return
			}
			sub.Close()
		}
	})
	b.StopTimer()

	cancel()
	bg.Wait()
	hub.Close()
}
