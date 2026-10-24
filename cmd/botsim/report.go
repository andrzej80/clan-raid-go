package main

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
)

// stats копит результаты одной горутины-бота; блокировок нет, потоки сливаются после завершения.
type stats struct {
	latencies []time.Duration
	codes     map[codes.Code]int
	// accepted — число ответов OK; повтор с тем же ключом тоже получает OK, поэтому это верхняя
	// оценка числа принятых ударов (уникальные удары считает БД).
	accepted int
	retries  int
}

func newStats() *stats { return &stats{codes: make(map[codes.Code]int)} }

func (s *stats) record(code codes.Code, d time.Duration) {
	s.latencies = append(s.latencies, d)
	s.codes[code]++
}

func (s *stats) merge(o *stats) {
	s.latencies = append(s.latencies, o.latencies...)
	for c, n := range o.codes {
		s.codes[c] += n
	}
	s.accepted += o.accepted
	s.retries += o.retries
}

// percentile возвращает p-й перцентиль (0..100) отсортированных значений; для пустого набора — 0.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p / 100)
	return sorted[idx]
}

// format готовит текстовый отчёт.
func (s *stats) format(elapsed time.Duration) string {
	var out strings.Builder
	sorted := slices.Clone(s.latencies)
	slices.Sort(sorted)

	total := len(sorted)
	fmt.Fprintf(&out, "rpc total:      %d in %s (%.0f rps)\n", total, elapsed.Round(time.Millisecond),
		float64(total)/elapsed.Seconds())
	fmt.Fprintf(&out, "OK responses:   %d\n", s.accepted)
	fmt.Fprintf(&out, "retries:        %d\n", s.retries)
	fmt.Fprintf(&out, "latency:        p50=%s p95=%s p99=%s max=%s\n",
		percentile(sorted, 50), percentile(sorted, 95), percentile(sorted, 99), percentile(sorted, 100))

	keys := make([]codes.Code, 0, len(s.codes))
	for c := range s.codes {
		keys = append(keys, c)
	}
	slices.Sort(keys)
	fmt.Fprintln(&out, "response codes:")
	for _, c := range keys {
		fmt.Fprintf(&out, "  %-20s %d\n", c, s.codes[c])
	}
	return out.String()
}
