package main

import (
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

const (
	retryBackoffMax = 5 * time.Second
	// Бюджет стартует с небольшим запасом, чтобы единичные сбои в начале прогона не оставались без повтора,
	// и копит не больше retryBudgetBurst токенов.
	retryBudgetInitial = 10
	retryBudgetBurst   = 100
)

// retryBudget ограничивает долю повторов от общего числа новых запросов. Без него open model во время
// сбоя сервера удваивает нагрузку: расписание продолжает выпускать новые запросы, а упавшие повторяются
// сверху. Каждый новый запрос кладёт ratio токена, каждый повтор забирает один; нет токена — повтора нет.
// Нулевой *retryBudget (ratio < 0) не ограничивает повторы.
type retryBudget struct {
	mu     sync.Mutex
	tokens float64
	ratio  float64
}

// newRetryBudget: ratio < 0 отключает ограничение, 0 запрещает повторы после начального запаса.
func newRetryBudget(ratio float64) *retryBudget {
	if ratio < 0 {
		return nil
	}
	return &retryBudget{tokens: retryBudgetInitial, ratio: ratio}
}

func (b *retryBudget) deposit() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.tokens = min(b.tokens+b.ratio, retryBudgetBurst)
	b.mu.Unlock()
}

func (b *retryBudget) withdraw() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// retryDelay — пауза перед повтором после попытки с номером attempt (с нуля): экспоненциальная,
// с джиттером в диапазоне [d/2, d], чтобы боты, упавшие в одну секунду, не повторяли синхронно.
// Если сервер прислал RetryInfo (например, rate limit), ждём не меньше этой задержки.
func retryDelay(attempt int, err error) time.Duration {
	d := min(retryBackoff<<attempt, retryBackoffMax)
	d = d/2 + time.Duration(rand.Int64N(int64(d/2)+1)) //nolint:gosec // джиттер, не криптография
	if hint, ok := retryInfo(err); ok {
		d = max(d, min(hint, retryBackoffMax))
	}
	return d
}

// retryInfo достаёт задержку из errdetails.RetryInfo ответа сервера.
func retryInfo(err error) (time.Duration, bool) {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.RetryInfo); ok {
			return info.GetRetryDelay().AsDuration(), true
		}
	}
	return 0, false
}
