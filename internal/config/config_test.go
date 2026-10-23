package config

import (
	"slices"
	"testing"
	"time"

	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

func TestLoad(t *testing.T) {
	t.Setenv("APP_HOST", "")
	t.Setenv("APP_PORT", "")
	t.Setenv("GRPC_REFLECTION", "")
	t.Setenv("DATABASE_URL", "postgres://test@localhost/raidboss")

	tests := []struct {
		name           string
		env            map[string]string
		wantAddr       string
		wantReflection bool
		wantErr        bool
	}{
		{name: "defaults", env: nil, wantAddr: ":9090", wantReflection: false},
		{name: "different host", env: map[string]string{"APP_HOST": "example.com"}, wantAddr: "example.com:9090"},
		{name: "custom port", env: map[string]string{"APP_PORT": "8080"}, wantAddr: ":8080"},
		{name: "port not a number", env: map[string]string{"APP_PORT": "abc"}, wantErr: true},
		{name: "port too small", env: map[string]string{"APP_PORT": "0"}, wantErr: true},
		{name: "port too big", env: map[string]string{"APP_PORT": "70000"}, wantErr: true},
		{name: "wrong GRPC_REFLECTION", env: map[string]string{"GRPC_REFLECTION": "70000"}, wantErr: true},
		{name: "enable by 1 GRPC_REFLECTION", env: map[string]string{"GRPC_REFLECTION": "1"}, wantReflection: true},
		{name: "enable by true GRPC_REFLECTION", env: map[string]string{"GRPC_REFLECTION": "true"}, wantReflection: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.env {
				t.Setenv(key, value)
			}

			cfg, err := Load()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Load() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr {
				return
			}

			if tc.wantAddr != "" && cfg.GRPCAddr != tc.wantAddr {
				t.Fatalf("Load() wrog parsed addres got = %v, expected %v", cfg.GRPCAddr, tc.wantAddr)
			}

			if cfg.GRPCReflection != tc.wantReflection {
				t.Fatalf("Load() wrog reflection got = %v, expected %v", cfg.GRPCReflection, tc.wantReflection)
			}
		})
	}
}

func TestLoadDB(t *testing.T) {
	const dsn = "postgres://test@localhost/raidboss"

	defaults := pg.DbConfig{
		Dsn:             dsn,
		MaxConns:        defaultMaxConns,
		MinConns:        defaultMinConns,
		MaxConnIdleTime: defaultMaxConnIdleTime,
		MaxConnLifetime: defaultMaxConnLifetime,
	}

	tests := []struct {
		name    string
		env     map[string]string
		want    pg.DbConfig
		wantErr bool
	}{
		{name: "defaults", env: map[string]string{"DATABASE_URL": dsn}, want: defaults},
		{name: "missing url", env: nil, wantErr: true},
		{
			name: "custom values",
			env: map[string]string{
				"DATABASE_URL":          dsn,
				"PG_MAX_CONNS":          "20",
				"PG_MIN_CONNS":          "2",
				"PG_MAX_CONN_IDLE_TIME": "1m",
				"PG_MAX_CONN_LIFETIME":  "1h",
			},
			want: pg.DbConfig{
				Dsn:             dsn,
				MaxConns:        20,
				MinConns:        2,
				MaxConnIdleTime: time.Minute,
				MaxConnLifetime: time.Hour,
			},
		},
		{
			name: "invalid values fall back to defaults",
			env: map[string]string{
				"DATABASE_URL":          dsn,
				"PG_MAX_CONNS":          "many",
				"PG_MIN_CONNS":          "1.5",
				"PG_MAX_CONN_IDLE_TIME": "soon",
				"PG_MAX_CONN_LIFETIME":  "10",
			},
			want: defaults,
		},
		{
			name: "out of int32 range falls back to default",
			env:  map[string]string{"DATABASE_URL": dsn, "PG_MAX_CONNS": "99999999999"},
			want: defaults,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, key := range []string{
				"DATABASE_URL", "PG_MAX_CONNS", "PG_MIN_CONNS", "PG_MAX_CONN_IDLE_TIME", "PG_MAX_CONN_LIFETIME",
			} {
				t.Setenv(key, "")
			}
			for key, value := range tc.env {
				t.Setenv(key, value)
			}

			got, err := loadDB()
			if (err != nil) != tc.wantErr {
				t.Fatalf("loadDB() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if got != tc.want {
				t.Fatalf("loadDB() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestLoadIncludesDB(t *testing.T) {
	t.Setenv("APP_HOST", "")
	t.Setenv("APP_PORT", "")
	t.Setenv("GRPC_REFLECTION", "")

	t.Run("database url is passed through", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://u@db/raidboss")
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.DB.Dsn != "postgres://u@db/raidboss" {
			t.Fatalf("Load().DB.Dsn = %q", cfg.DB.Dsn)
		}
	})

	t.Run("missing database url fails Load", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "")
		if _, err := Load(); err == nil {
			t.Fatal("Load() must fail without DATABASE_URL")
		}
	})
}

func TestLoadKafka(t *testing.T) {
	tests := []struct {
		name        string
		brokers     string
		topic       string
		wantBrokers []string
		wantTopic   string
	}{
		{"not set", "", "", nil, "raid.hits"},
		{"one broker", "kafka:9092", "", []string{"kafka:9092"}, "raid.hits"},
		{"several with spaces and empties", " a:9092, b:9092 ,,", "", []string{"a:9092", "b:9092"}, "raid.hits"},
		{"custom topic", "kafka:9092", "hits.test", []string{"kafka:9092"}, "hits.test"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KAFKA_BROKERS", tc.brokers)
			t.Setenv("KAFKA_HITS_TOPIC", tc.topic)

			got := loadKafka()
			if !slices.Equal(got.Brokers, tc.wantBrokers) || got.HitsTopic != tc.wantTopic {
				t.Fatalf("loadKafka() = %+v, want brokers %v topic %q", got, tc.wantBrokers, tc.wantTopic)
			}
		})
	}
}

func TestPublicServices(t *testing.T) {
	t.Parallel()

	const (
		health    = "grpc.health.v1.Health"
		reflect   = "grpc.reflection.v1.ServerReflection"
		reflectV1 = "grpc.reflection.v1alpha.ServerReflection"
		raid      = "raidboss.v1.RaidService"
	)

	off := (&Config{GRPCReflection: false}).PublicServices()
	if !slices.Contains(off, health) || slices.Contains(off, reflect) || slices.Contains(off, reflectV1) {
		t.Fatalf("reflection off: got %v, want only health", off)
	}

	on := (&Config{GRPCReflection: true}).PublicServices()
	for _, want := range []string{health, reflect, reflectV1} {
		if !slices.Contains(on, want) {
			t.Fatalf("reflection on: %v must contain %s", on, want)
		}
	}
	if slices.Contains(on, raid) {
		t.Fatalf("business service must never be public: %v", on)
	}
}
