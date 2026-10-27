package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"google.golang.org/grpc/codes"
)

const (
	histMin    = int64(time.Microsecond)
	histMax    = int64(time.Minute)
	histDigits = 2 // 1% точности: хватает для перцентилей, а память на бота остаётся небольшой
)

// stats копит результаты запросов. Потокобезопасна: в open model в неё пишут тысячи горутин,
// в closed model у каждого бота своя, а потоки сливаются после завершения.
type stats struct {
	mu    sync.Mutex
	epoch time.Time // начало прогона: от него считаются секундные корзины RPS
	hist  *hdrhistogram.Histogram
	codes map[codes.Code]int
	// perSecond[i] — число ответов, полученных в i-ю секунду прогона.
	perSecond []int
	// accepted — число ответов OK; повтор с тем же ключом тоже получает OK, поэтому это верхняя
	// оценка числа принятых ударов (уникальные удары считает БД).
	accepted int
	retries  int
	// retriesDenied — повторы, которые бот не сделал, потому что бюджет повторов исчерпан.
	retriesDenied int
	dropped       int         // open model: запросы, не отправленные из-за лимита maxInflight
	watch         *watchStats // nil, если подписчиков нет
}

func newStats(epoch time.Time) *stats {
	return &stats{
		epoch: epoch,
		hist:  hdrhistogram.New(histMin, histMax, histDigits),
		codes: make(map[codes.Code]int),
	}
}

func (s *stats) record(code codes.Code, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Значения вне диапазона гистограммы прижимаем к границам: запрос не должен пропасть из отчёта.
	_ = s.hist.RecordValue(min(max(int64(d), histMin), histMax)) //nolint:gosec // значение прижато к диапазону, ошибки быть не может
	s.codes[code]++

	sec := max(int(time.Since(s.epoch)/time.Second), 0)
	if sec >= len(s.perSecond) {
		s.perSecond = append(s.perSecond, make([]int, sec+1-len(s.perSecond))...)
	}
	s.perSecond[sec]++
}

func (s *stats) markAccepted() {
	s.mu.Lock()
	s.accepted++
	s.mu.Unlock()
}

func (s *stats) markRetry() {
	s.mu.Lock()
	s.retries++
	s.mu.Unlock()
}

func (s *stats) markRetryDenied() {
	s.mu.Lock()
	s.retriesDenied++
	s.mu.Unlock()
}

func (s *stats) markDropped() {
	s.mu.Lock()
	s.dropped++
	s.mu.Unlock()
}

// merge добавляет o в s. o после слияния менять нельзя.
func (s *stats) merge(o *stats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o.mu.Lock()
	defer o.mu.Unlock()

	s.hist.Merge(o.hist)
	for c, n := range o.codes {
		s.codes[c] += n
	}
	if len(o.perSecond) > len(s.perSecond) {
		s.perSecond = append(s.perSecond, make([]int, len(o.perSecond)-len(s.perSecond))...)
	}
	for i, n := range o.perSecond {
		s.perSecond[i] += n
	}
	s.accepted += o.accepted
	s.retries += o.retries
	s.retriesDenied += o.retriesDenied
	s.dropped += o.dropped
}

// latencyReport — задержки в миллисекундах.
type latencyReport struct {
	Mean float64 `json:"mean"`
	P50  float64 `json:"p50"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p99_9"`
	Max  float64 `json:"max"`
}

// report — итог прогона; его же сериализует флаг --json.
type report struct {
	Conns         int            `json:"conns"`
	ElapsedSec    float64        `json:"elapsed_sec"`
	RPCTotal      int64          `json:"rpc_total"`
	RPS           float64        `json:"rps"`
	OK            int            `json:"ok_responses"`
	Retries       int            `json:"retries"`
	RetriesDenied int            `json:"retries_denied"`
	Dropped       int            `json:"dropped"`
	LatencyMs     latencyReport  `json:"latency_ms"`
	Codes         map[string]int `json:"codes"`
	RPSPerSecond  []int          `json:"rps_per_second"`
	Watch         *watchReport   `json:"watch,omitempty"`
}

func ms(ns float64) float64 { return ns / float64(time.Millisecond) }

func (s *stats) report(elapsed time.Duration) report {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := func(p float64) float64 { return ms(float64(s.hist.ValueAtQuantile(p))) }
	r := report{
		ElapsedSec:    elapsed.Seconds(),
		RPCTotal:      s.hist.TotalCount(),
		OK:            s.accepted,
		Retries:       s.retries,
		RetriesDenied: s.retriesDenied,
		Dropped:       s.dropped,
		LatencyMs: latencyReport{
			Mean: ms(s.hist.Mean()), P50: q(50), P95: q(95), P99: q(99), P999: q(99.9), Max: ms(float64(s.hist.Max())),
		},
		Codes:        make(map[string]int, len(s.codes)),
		RPSPerSecond: slices.Clone(s.perSecond),
	}
	if s.watch != nil {
		r.Watch = s.watch.report()
	}
	if elapsed > 0 {
		r.RPS = float64(r.RPCTotal) / elapsed.Seconds()
	}
	for c, n := range s.codes {
		r.Codes[c.String()] = n
	}
	return r
}

// format готовит текстовый отчёт.
func (r report) format() string {
	var out strings.Builder
	elapsed := time.Duration(r.ElapsedSec * float64(time.Second)).Round(time.Millisecond)
	fmt.Fprintf(&out, "rpc total:      %d in %s (%.0f rps)\n", r.RPCTotal, elapsed, r.RPS)
	fmt.Fprintf(&out, "connections:    %d\n", r.Conns)
	fmt.Fprintf(&out, "OK responses:   %d\n", r.OK)
	fmt.Fprintf(&out, "retries:        %d\n", r.Retries)
	if r.RetriesDenied > 0 {
		fmt.Fprintf(&out, "retries denied: %d (retry budget exhausted)\n", r.RetriesDenied)
	}
	if r.Dropped > 0 {
		fmt.Fprintf(&out, "dropped:        %d (max-inflight reached)\n", r.Dropped)
	}
	l := r.LatencyMs
	fmt.Fprintf(&out, "latency (ms):   mean=%.2f p50=%.2f p95=%.2f p99=%.2f p99.9=%.2f max=%.2f\n",
		l.Mean, l.P50, l.P95, l.P99, l.P999, l.Max)
	// Первая и последняя секунды неполные, в min/max их не берём.
	if full := r.RPSPerSecond; len(full) > 2 {
		full = full[1 : len(full)-1]
		fmt.Fprintf(&out, "rps per second: min=%d max=%d (full seconds)\n", slices.Min(full), slices.Max(full))
	}

	names := make([]string, 0, len(r.Codes))
	for name := range r.Codes {
		names = append(names, name)
	}
	slices.Sort(names)
	if r.Watch != nil {
		out.WriteString(r.Watch.format())
	}
	fmt.Fprintln(&out, "response codes:")
	for _, name := range names {
		fmt.Fprintf(&out, "  %-20s %d\n", name, r.Codes[name])
	}
	return out.String()
}

func (r report) json() ([]byte, error) { return json.MarshalIndent(r, "", "  ") }
