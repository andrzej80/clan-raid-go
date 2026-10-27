// Package breaker — предохранитель (circuit breaker): после серии подряд идущих сбоев зависимости
// перестаёт вызывать её и отвечает отказом сразу.
//
// Выход из разомкнутого состояния определяет не время и не трафик, а внешняя проверка (Run): пока она
// не подтвердила, что зависимость жива, вызовы не пропускаются. После этого предохранитель пропускает
// малую долю трафика (ProbeRatio, но не реже одного вызова в MinProbeInterval, чтобы при малой нагрузке
// проб хватало) и замыкается, когда CloseAfter таких вызовов подряд прошли успешно.
package breaker

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

// ErrOpen возвращается, пока вызов не выполнялся: предохранитель разомкнут или вызов не попал в пробную долю.
var ErrOpen = errors.New("circuit breaker is open")

// State — состояние предохранителя.
type State int

const (
	Closed   State = iota // вызовы идут как обычно
	Open                  // вызовы отклоняются без обращения к зависимости
	HalfOpen              // пропускается доля вызовов (ProbeRatio), остальные отклоняются
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Config задаёт пороги. Нулевые значения заменяются на разумные по умолчанию.
type Config struct {
	// FailureThreshold — сколько сбоев подряд размыкают предохранитель (по умолчанию 20).
	FailureThreshold int
	// ProbeRatio — доля вызовов, пропускаемых в half-open (по умолчанию 0.05).
	ProbeRatio float64
	// MinProbeInterval — не реже чем раз в этот интервал в half-open пропускается один вызов, даже если
	// он не попал в ProbeRatio (по умолчанию 500 мс). Без этого при малом RPS замыкание занимало бы минуты.
	MinProbeInterval time.Duration
	// CloseAfter — сколько успешных пробных вызовов замыкают предохранитель (по умолчанию 5).
	CloseAfter int
	// MaxInconclusive — сколько пробных вызовов без вывода (отмена или дедлайн клиента) допустимо
	// в одном half-open, прежде чем вернуться в Open (по умолчанию 10). Без лимита предохранитель
	// мог бы остаться в half-open навсегда, если клиентские дедлайны короче времени ответа.
	MaxInconclusive int
	// OnStateChange вызывается при каждой смене состояния, без удержания блокировки.
	OnStateChange func(from, to State)
	// SettleTime — через сколько времени после входа в half-open хватает одного успешного вызова, чтобы
	// замкнуться, если CloseAfter набрать не успели (по умолчанию 1 с). Нужно при малой нагрузке.
	SettleTime time.Duration
	// Now подменяет часы в тестах.
	Now func() time.Time
	// Rand подменяет источник случайных чисел [0, 1) в тестах.
	Rand func() float64
}

// Breaker безопасен для конкурентного использования.
type Breaker struct {
	cfg Config

	mu           sync.Mutex
	state        State
	epoch        uint64    // растёт при каждой смене состояния: результаты «чужой» эпохи игнорируются
	failures     int       // подряд идущие сбои в Closed
	successes    int       // успешные пробные вызовы в текущем HalfOpen
	inconclusive int       // пробные вызовы без вывода в текущем HalfOpen
	lastProbe    time.Time // когда в half-open последний раз пропущен пробный вызов
	halfOpenAt   time.Time // когда предохранитель перешёл в текущий half-open
}

func New(cfg Config) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 20
	}
	if cfg.ProbeRatio <= 0 || cfg.ProbeRatio > 1 {
		cfg.ProbeRatio = 0.05
	}
	if cfg.CloseAfter <= 0 {
		cfg.CloseAfter = 5
	}
	if cfg.MaxInconclusive <= 0 {
		cfg.MaxInconclusive = 10
	}
	if cfg.SettleTime <= 0 {
		cfg.SettleTime = time.Second
	}
	if cfg.MinProbeInterval <= 0 {
		cfg.MinProbeInterval = 500 * time.Millisecond
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.Float64
	}
	return &Breaker{cfg: cfg}
}

// State возвращает текущее состояние (для метрик и тестов).
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Do выполняет fn, если предохранитель пропускает вызов, иначе сразу возвращает ErrOpen.
// isFailure решает, считать ли ошибку fn сбоем зависимости: отмена по вине клиента сбоем не является
// (и не доказательством, что зависимость жива). nil считает сбоем любую ошибку.
func (b *Breaker) Do(fn func() error, isFailure func(error) bool) error {
	epoch, probe, err := b.allow()
	if err != nil {
		return err
	}

	err = fn()
	failed := err != nil && (isFailure == nil || isFailure(err))
	b.done(epoch, probe, outcome(err, failed))
	return err
}

type result int

const (
	resOK           result = iota
	resFailed              // сбой зависимости
	resInconclusive        // ошибка клиента: о зависимости ничего не известно
)

func outcome(err error, failed bool) result {
	switch {
	case err == nil:
		return resOK
	case failed:
		return resFailed
	default:
		return resInconclusive
	}
}

// allow решает, пропускать ли вызов; probe — это пробный вызов в HalfOpen.
func (b *Breaker) allow() (epoch uint64, probe bool, _ error) {
	b.mu.Lock()
	epoch, probe, closed, err := b.allowLocked()
	b.mu.Unlock()

	if closed {
		b.notify(HalfOpen, Closed)
	}
	return epoch, probe, err
}

// allowLocked — тело allow под b.mu; closed сообщает, что предохранитель только что замкнулся.
func (b *Breaker) allowLocked() (epoch uint64, probe, closed bool, _ error) {
	switch b.state {
	case Closed:
		return b.epoch, false, false, nil
	case Open:
		// разомкнут: из этого состояния выводит только проверка живости (HalfOpen)
	case HalfOpen:
		now := b.cfg.Now()
		// При малой нагрузке успешных проб набирается мало: один успех и SettleTime без сбоев
		// считаются достаточными. Условие проверяется лениво, на следующем вызове.
		if b.successes > 0 && now.Sub(b.halfOpenAt) >= b.cfg.SettleTime {
			b.transition(Closed)
			return b.epoch, false, true, nil
		}
		if b.cfg.Rand() < b.cfg.ProbeRatio || now.Sub(b.lastProbe) >= b.cfg.MinProbeInterval {
			b.lastProbe = now
			return b.epoch, true, false, nil
		}
	}
	return 0, false, false, ErrOpen
}

// done учитывает результат вызова. Вызов из прошлой эпохи (состояние с тех пор менялось) игнорируется.
func (b *Breaker) done(epoch uint64, probe bool, res result) {
	b.mu.Lock()
	if epoch != b.epoch {
		b.mu.Unlock()
		return
	}

	from := b.state
	switch {
	case probe:
		b.countProbe(res)
	case res == resFailed:
		b.failures++
		if b.failures >= b.cfg.FailureThreshold {
			b.transition(Open)
		}
	case res == resOK:
		b.failures = 0
	}
	to := b.state
	b.mu.Unlock()

	if from != to {
		b.notify(from, to)
	}
}

// countProbe: сбой пробы возвращает в Open, достаточное число успехов замыкает предохранитель.
// Вызывается под b.mu.
func (b *Breaker) countProbe(res result) {
	switch res {
	case resFailed:
		b.transition(Open)
	case resOK:
		b.successes++
		if b.successes >= b.cfg.CloseAfter {
			b.transition(Closed)
		}
	case resInconclusive:
		b.inconclusive++
		if b.inconclusive >= b.cfg.MaxInconclusive {
			b.transition(Open)
		}
	}
}

// transition меняет состояние и обнуляет счётчики. Вызывается под b.mu.
func (b *Breaker) transition(to State) {
	b.state = to
	b.epoch++
	b.failures, b.successes, b.inconclusive = 0, 0, 0
	b.lastProbe = time.Time{}
	if to == HalfOpen {
		b.halfOpenAt = b.cfg.Now()
	}
}

// HalfOpen переводит разомкнутый предохранитель в half-open: дальше пропускается доля трафика.
// Возвращает false, если предохранитель не был разомкнут.
func (b *Breaker) HalfOpen() bool {
	b.mu.Lock()
	if b.state != Open {
		b.mu.Unlock()
		return false
	}
	b.transition(HalfOpen)
	b.mu.Unlock()

	b.notify(Open, HalfOpen)
	return true
}

func (b *Breaker) notify(from, to State) {
	if b.cfg.OnStateChange != nil {
		b.cfg.OnStateChange(from, to)
	}
}

// Run раз в interval проверяет зависимость, пока предохранитель разомкнут: probe с таймаутом timeout.
// Если проверка прошла, предохранитель переходит в half-open. Блокируется до отмены ctx.
func (b *Breaker) Run(ctx context.Context, interval, timeout time.Duration, probe func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if b.State() != Open {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		err := probe(probeCtx)
		cancel()
		if err == nil {
			b.HalfOpen()
		}
	}
}
