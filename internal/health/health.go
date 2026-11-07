// Package health — HTTP-пробы сервисов: liveness (/livez), readiness (/readyz) и pprof.
//
// Liveness отвечает на вопрос «процесс не завис»: главный цикл сервиса регулярно отмечается в
// Heartbeat, и если отметок нет дольше MaxStall, проба отвечает 503. Зависимости liveness не
// проверяет: иначе недоступный Postgres перезапустил бы все экземпляры разом, а после рестарта
// они упали бы так же.
//
// Readiness проверяет зависимости (Postgres, Kafka) при каждом запросе, у каждой свой таймаут.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultMaxStall — сколько главный цикл может не отмечаться, прежде чем liveness ответит 503.
// Должно быть больше самого долгого штатного шага цикла (повтор пачки ждёт до 5 с, отправка в
// Kafka при недоступном брокере — до десятков секунд).
const DefaultMaxStall = time.Minute

// Heartbeat — отметка живости главного цикла. Нулевое значение готово к работе; методы
// безопасны для nil, поэтому компоненты могут вызывать Beat, не проверяя, включены ли пробы.
type Heartbeat struct {
	last atomic.Int64 // UnixNano последней отметки, 0 — отметок не было
}

// NewHeartbeat создаёт отметку, засчитанную на момент создания: до первой итерации цикла
// сервис считается живым.
func NewHeartbeat() *Heartbeat {
	h := &Heartbeat{}
	h.Beat()
	return h
}

// Beat отмечает, что цикл сделал шаг.
func (h *Heartbeat) Beat() {
	if h == nil {
		return
	}
	h.last.Store(time.Now().UnixNano())
}

// Last возвращает время последней отметки; нулевое время, если отметок не было.
func (h *Heartbeat) Last() time.Time {
	if h == nil {
		return time.Time{}
	}
	n := h.last.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// Check — проверка одной зависимости для readiness.
type Check struct {
	Name    string
	Timeout time.Duration
	Ping    func(ctx context.Context) error
}

// Config настраивает пробы.
type Config struct {
	// Heartbeat главного цикла. nil — liveness всегда 200 (сервис без собственного цикла, как api).
	Heartbeat *Heartbeat
	// MaxStall; 0 — DefaultMaxStall.
	MaxStall time.Duration
	// Checks — зависимости для readiness.
	Checks []Check
}

type checkResult struct {
	Name  string `json:"name"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type readyResponse struct {
	Ready  bool          `json:"ready"`
	Checks []checkResult `json:"checks"`
}

type liveResponse struct {
	Live          bool    `json:"live"`
	SinceBeatSecs float64 `json:"since_beat_seconds,omitempty"`
}

// Handler возвращает mux с /livez, /readyz и /debug/pprof/*.
func Handler(cfg Config) http.Handler {
	if cfg.MaxStall <= 0 {
		cfg.MaxStall = DefaultMaxStall
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		resp, code := live(cfg, time.Now())
		writeJSON(w, code, resp)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		resp := ready(r.Context(), cfg.Checks)
		code := http.StatusOK
		if !resp.Ready {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, resp)
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func live(cfg Config, now time.Time) (liveResponse, int) {
	if cfg.Heartbeat == nil {
		return liveResponse{Live: true}, http.StatusOK
	}
	since := now.Sub(cfg.Heartbeat.Last())
	resp := liveResponse{Live: since <= cfg.MaxStall, SinceBeatSecs: since.Seconds()}
	if !resp.Live {
		return resp, http.StatusServiceUnavailable
	}
	return resp, http.StatusOK
}

// ready опрашивает зависимости параллельно: общий ответ не дольше самого долгого таймаута.
func ready(ctx context.Context, checks []Check) readyResponse {
	results := make([]checkResult, len(checks))
	var wg sync.WaitGroup
	for i, c := range checks {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, c.Timeout)
			defer cancel()
			results[i] = checkResult{Name: c.Name, OK: true}
			if err := c.Ping(cctx); err != nil {
				results[i] = checkResult{Name: c.Name, Error: err.Error()}
			}
		})
	}
	wg.Wait()

	resp := readyResponse{Ready: true, Checks: results}
	for _, r := range results {
		resp.Ready = resp.Ready && r.OK
	}
	return resp
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Debug("write probe response", "err", err) // клиент пробы ушёл — сделать нечего
	}
}

// shutdownTimeout — сколько ждём завершения запросов к пробам при остановке.
const shutdownTimeout = 2 * time.Second

// Serve запускает HTTP-сервер проб на addr и останавливает его при отмене ctx. Возвращает
// функцию, которая ждёт остановки сервера; ошибка прослушивания порта возвращается сразу.
func Serve(ctx context.Context, addr string, h http.Handler) (wait func(), err error) {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
	}
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("probe server listen %s: %w", addr, err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("probe server", "addr", addr, "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil {
			slog.Warn("probe server shutdown", "err", err)
		}
	}()
	slog.Info("probe server listening", "addr", addr)
	return func() { <-done }, nil
}
