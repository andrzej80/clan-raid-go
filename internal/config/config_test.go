package config

import (
	"slices"
	"testing"
	"time"

	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

func TestLoadKafkaConsumerDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test@localhost/raidboss")
	t.Setenv("KAFKA_BROKERS", "a:9092, b:9092")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	k := cfg.Kafka
	if !slices.Equal(k.Brokers, []string{"a:9092", "b:9092"}) {
		t.Fatalf("brokers = %v", k.Brokers)
	}
	if k.HitsDLQTopic != "raid.hits.dlq" || k.ConsumerGroup != "hits-consumer" ||
		k.BatchSize != 500 || k.BatchTimeout != 50*time.Millisecond {
		t.Fatalf("unexpected defaults: %+v", k)
	}
}

func TestLoadAttackMode(t *testing.T) {
	tests := []struct {
		env     string
		want    string
		wantErr bool
	}{
		{env: "", want: AttackModeKafka},
		{env: "kafka", want: AttackModeKafka},
		{env: "sync", want: AttackModeSync},
		{env: "sync-full", want: AttackModeSyncFull},
		{env: "async", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://test@localhost/raidboss")
			t.Setenv("ATTACK_MODE", tt.env)

			cfg, err := Load()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Load() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && cfg.AttackMode != tt.want {
				t.Fatalf("AttackMode = %q, want %q", cfg.AttackMode, tt.want)
			}
		})
	}
}

func TestLoadRedis(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test@localhost/raidboss")
	t.Setenv("REDIS_URL", "redis://redis:6379")
	t.Setenv("REDIS_POOL_SIZE", "")
	t.Setenv("REDIS_READ_TIMEOUT", "250ms")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	r := cfg.Redis
	if r.URL != "redis://redis:6379" || r.PoolSize != 20 ||
		r.DialTimeout != time.Second || r.ReadTimeout != 250*time.Millisecond || r.WriteTimeout != 500*time.Millisecond {
		t.Fatalf("unexpected redis config: %+v", r)
	}
}

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

func TestLoadKafkaEventTopics(t *testing.T) {
	tests := []struct {
		name                                   string
		events, eventsDLQ, rewards, rewardsGrp string
		want                                   KafkaConfig
	}{
		{
			name: "defaults",
			want: KafkaConfig{
				EventsTopic: "raid.events", EventsDLQTopic: "raid.events.dlq",
				RewardsTopic: "rewards.granted", RewardsGroup: "rewards-worker",
			},
		},
		{
			name:   "overrides",
			events: "events.test", eventsDLQ: "events.dlq.test", rewards: "rewards.test", rewardsGrp: "rw",
			want: KafkaConfig{
				EventsTopic: "events.test", EventsDLQTopic: "events.dlq.test",
				RewardsTopic: "rewards.test", RewardsGroup: "rw",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KAFKA_EVENTS_TOPIC", tc.events)
			t.Setenv("KAFKA_EVENTS_DLQ_TOPIC", tc.eventsDLQ)
			t.Setenv("KAFKA_REWARDS_TOPIC", tc.rewards)
			t.Setenv("REWARDS_CONSUMER_GROUP", tc.rewardsGrp)

			got := loadKafka()
			if got.EventsTopic != tc.want.EventsTopic || got.EventsDLQTopic != tc.want.EventsDLQTopic ||
				got.RewardsTopic != tc.want.RewardsTopic || got.RewardsGroup != tc.want.RewardsGroup {
				t.Fatalf("loadKafka() = %+v, want topics %+v", got, tc.want)
			}
		})
	}
}

func TestLoadOutbox(t *testing.T) {
	tests := []struct {
		name         string
		batch, poll  string
		wantBatch    int
		wantInterval time.Duration
	}{
		{"defaults", "", "", 100, 200 * time.Millisecond},
		{"overrides", "10", "1s", 10, time.Second},
		// Нулевые и отрицательные значения заменяются умолчаниями: иначе relay крутился бы вхолостую.
		{"zero batch and interval", "0", "0s", 100, 200 * time.Millisecond},
		{"negative batch and interval", "-5", "-1s", 100, 200 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OUTBOX_BATCH_SIZE", tc.batch)
			t.Setenv("OUTBOX_POLL_INTERVAL", tc.poll)

			got := loadOutbox()
			if got.BatchSize != tc.wantBatch || got.PollInterval != tc.wantInterval {
				t.Fatalf("loadOutbox() = %+v, want batch %d interval %v", got, tc.wantBatch, tc.wantInterval)
			}
		})
	}
}

func TestLoadScheduler(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want time.Duration
	}{
		{"default", "", 10 * time.Second},
		{"override", "3s", 3 * time.Second},
		{"zero falls back", "0s", 10 * time.Second},
		{"negative falls back", "-1s", 10 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://test@localhost/raidboss")
			t.Setenv("SCHEDULER_INTERVAL", tc.env)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Scheduler.Interval != tc.want {
				t.Fatalf("scheduler interval = %v, want %v", cfg.Scheduler.Interval, tc.want)
			}
		})
	}
}

func TestLoadWatch(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test@localhost/raidboss")
	t.Setenv("WATCH_BUFFER_SIZE", "")
	t.Setenv("WATCH_UPDATE_INTERVAL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Watch.BufferSize != 16 || cfg.Watch.UpdateInterval != 250*time.Millisecond {
		t.Fatalf("unexpected watch defaults: %+v", cfg.Watch)
	}

	t.Setenv("WATCH_BUFFER_SIZE", "64")
	t.Setenv("WATCH_UPDATE_INTERVAL", "1s")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Watch.BufferSize != 64 || cfg.Watch.UpdateInterval != time.Second {
		t.Fatalf("unexpected watch config: %+v", cfg.Watch)
	}
}

func TestLoadProbe(t *testing.T) {
	tests := []struct {
		name      string
		addr      string
		stall     string
		wantAddr  string
		wantStall time.Duration
	}{
		{"defaults", "", "", ":8081", time.Minute},
		{"override", "127.0.0.1:9100", "30s", "127.0.0.1:9100", 30 * time.Second},
		{"non-positive stall falls back", "", "0s", ":8081", time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://test@localhost/raidboss")
			t.Setenv("PROBE_ADDR", tc.addr)
			t.Setenv("LIVENESS_MAX_STALL", tc.stall)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Probe.Addr != tc.wantAddr || cfg.Probe.MaxStall != tc.wantStall {
				t.Fatalf("probe = %+v, want addr %q stall %v", cfg.Probe, tc.wantAddr, tc.wantStall)
			}
		})
	}
}
