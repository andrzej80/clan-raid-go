package main

import "testing"

func TestConnPoolRoundRobin(t *testing.T) {
	t.Parallel()

	p, err := newConnPool("passthrough:///unused", 3) // NewClient не подключается, пока нет вызовов
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })

	seen := map[any]int{}
	for range 9 {
		seen[p.pick()]++
	}
	if len(seen) != 3 {
		t.Fatalf("used %d distinct connections, want 3", len(seen))
	}
	for c, n := range seen {
		if n != 3 {
			t.Fatalf("connection %p got %d calls, want 3", c, n)
		}
	}
}

func TestConnPoolRejectsNonPositive(t *testing.T) {
	t.Parallel()

	if _, err := newConnPool("passthrough:///unused", 0); err == nil {
		t.Fatal("expected an error for conns=0")
	}
}
