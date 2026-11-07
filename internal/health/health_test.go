package health

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func get(t *testing.T, h http.Handler, path string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s: decode body %q: %v", path, rec.Body.String(), err)
	}
	return rec.Code, body
}

func TestHeartbeat_NilIsSafe(t *testing.T) {
	t.Parallel()

	var h *Heartbeat
	h.Beat()
	if !h.Last().IsZero() {
		t.Fatal("nil heartbeat must report zero time")
	}
}

func TestLive_WithoutHeartbeatIsAlwaysLive(t *testing.T) {
	t.Parallel()

	code, body := get(t, Handler(Config{}), "/livez")
	if code != http.StatusOK || body["live"] != true {
		t.Fatalf("got %d %v, want 200 live", code, body)
	}
}

func TestLive_StaleHeartbeat(t *testing.T) {
	t.Parallel()

	hb := NewHeartbeat()
	cfg := Config{Heartbeat: hb, MaxStall: time.Minute}

	if _, code := live(cfg, time.Now().Add(30*time.Second)); code != http.StatusOK {
		t.Fatalf("fresh beat: got %d, want 200", code)
	}
	resp, code := live(cfg, time.Now().Add(2*time.Minute))
	if code != http.StatusServiceUnavailable || resp.Live {
		t.Fatalf("stale beat: got %d %+v, want 503", code, resp)
	}

	hb.Beat()
	if code, _ := get(t, Handler(cfg), "/livez"); code != http.StatusOK {
		t.Fatalf("after beat: got %d, want 200", code)
	}
}

func TestReady_AllChecksMustPass(t *testing.T) {
	t.Parallel()

	ok := Check{Name: "postgres", Timeout: time.Second, Ping: func(context.Context) error { return nil }}
	bad := Check{Name: "kafka", Timeout: time.Second, Ping: func(context.Context) error { return errors.New("down") }}

	if code, _ := get(t, Handler(Config{Checks: []Check{ok}}), "/readyz"); code != http.StatusOK {
		t.Fatalf("healthy: got %d, want 200", code)
	}

	code, body := get(t, Handler(Config{Checks: []Check{ok, bad}}), "/readyz")
	if code != http.StatusServiceUnavailable || body["ready"] != false {
		t.Fatalf("one failing: got %d %v, want 503", code, body)
	}
	checks, _ := body["checks"].([]any)
	if len(checks) != 2 {
		t.Fatalf("want 2 check results, got %v", body["checks"])
	}
	second, _ := checks[1].(map[string]any)
	if second["name"] != "kafka" || second["error"] != "down" {
		t.Fatalf("kafka result: got %v", second)
	}
}

func TestReady_CheckTimeoutIsApplied(t *testing.T) {
	t.Parallel()

	slow := Check{Name: "slow", Timeout: 20 * time.Millisecond, Ping: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	start := time.Now()
	resp := ready(t.Context(), []Check{slow})
	if resp.Ready {
		t.Fatal("timed out check must not be ready")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("check took %v, timeout not applied", elapsed)
	}
}

func TestHandler_ServesPprof(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	Handler(Config{}).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/debug/pprof/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("pprof index: got %d, want 200", rec.Code)
	}
}

func TestServe_StopsOnCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	wait, err := Serve(ctx, "127.0.0.1:0", Handler(Config{}))
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	done := make(chan struct{})
	go func() { wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop after cancel")
	}
}

func TestServe_ListenErrorIsReturned(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lis.Close() }()

	if _, err := Serve(t.Context(), lis.Addr().String(), Handler(Config{})); err == nil {
		t.Fatal("want error for busy port")
	}
}
