package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"

	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

const (
	defaultMaxConns        = 5
	defaultMinConns        = 0
	defaultMaxConnIdleTime = 5 * time.Minute
	defaultMaxConnLifetime = 30 * time.Minute
)

const (
	defaultHitsTopic    = "raid.hits"
	defaultHitsDLQTopic = "raid.hits.dlq"
	defaultEventsTopic  = "raid.events"
	defaultHitsGroup    = "hits-consumer"
	defaultBatchSize    = 500
	defaultBatchTimeout = 50 * time.Millisecond
)

// KafkaConfig: Brokers может быть пустым (raidctl Kafka не нужна), api проверяет это при старте.
type KafkaConfig struct {
	Brokers      []string
	HitsTopic    string
	HitsDLQTopic string
	// EventsTopic — топик, куда outbox-relay переносит события рейдов; имя пишется в outbox.topic.
	EventsTopic string
	// ConsumerGroup и параметры пачки используются только hits-consumer.
	ConsumerGroup string
	BatchSize     int
	BatchTimeout  time.Duration
}

type Config struct {
	GRPCAddr       string
	GRPCReflection bool
	DB             pg.DbConfig
	Kafka          KafkaConfig
}

// PublicServices — сервисы, которым не нужна авторизация
func (c *Config) PublicServices() []string {
	services := []string{healthpb.Health_ServiceDesc.ServiceName}
	if c.GRPCReflection {
		services = append(services,
			reflectionv1.ServerReflection_ServiceDesc.ServiceName,
			reflectionv1alpha.ServerReflection_ServiceDesc.ServiceName,
		)
	}
	return services
}

func Load() (*Config, error) {
	host := os.Getenv("APP_HOST")
	appPort := os.Getenv("APP_PORT")
	if appPort == "" {
		appPort = "9090"
	}
	numericPort, err := strconv.Atoi(appPort)
	if err != nil {
		return nil, fmt.Errorf("invalid APP_PORT %q: %w", appPort, err)
	}
	if numericPort > 65535 || numericPort < 1 {
		return nil, fmt.Errorf("APP_PORT %d out of range 1..65535", numericPort)
	}
	cfg := &Config{
		GRPCAddr:       net.JoinHostPort(host, appPort),
		GRPCReflection: false,
	}

	isDevMode := os.Getenv("GRPC_REFLECTION")
	if isDevMode == "" {
		cfg.GRPCReflection = false
	} else {
		parsed, err := strconv.ParseBool(isDevMode)
		if err != nil {
			return nil, fmt.Errorf("invalid GRPC_REFLECTION %q: %w", isDevMode, err)
		}
		cfg.GRPCReflection = parsed
	}

	db, err := loadDB()
	if err != nil {
		return nil, err
	}
	cfg.DB = db
	cfg.Kafka = loadKafka()
	return cfg, nil
}

func loadKafka() KafkaConfig {
	cfg := KafkaConfig{
		HitsTopic:     defaultHitsTopic,
		HitsDLQTopic:  defaultHitsDLQTopic,
		EventsTopic:   defaultEventsTopic,
		ConsumerGroup: defaultHitsGroup,
		BatchSize:     int(getEnvInt("CONSUMER_BATCH_SIZE", defaultBatchSize)),
		BatchTimeout:  getEnvDuration("CONSUMER_BATCH_TIMEOUT", defaultBatchTimeout),
	}
	for broker := range strings.SplitSeq(os.Getenv("KAFKA_BROKERS"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			cfg.Brokers = append(cfg.Brokers, broker)
		}
	}
	if topic := os.Getenv("KAFKA_HITS_TOPIC"); topic != "" {
		cfg.HitsTopic = topic
	}
	if topic := os.Getenv("KAFKA_HITS_DLQ_TOPIC"); topic != "" {
		cfg.HitsDLQTopic = topic
	}
	if group := os.Getenv("KAFKA_CONSUMER_GROUP"); group != "" {
		cfg.ConsumerGroup = group
	}
	if cfg.BatchSize < 1 {
		cfg.BatchSize = defaultBatchSize
	}
	return cfg
}

func loadDB() (pg.DbConfig, error) {
	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		return pg.DbConfig{}, errors.New("DATABASE_URL is required")
	}

	cfg := pg.DbConfig{
		Dsn:             dbUrl,
		MaxConns:        getEnvInt("PG_MAX_CONNS", defaultMaxConns),
		MinConns:        getEnvInt("PG_MIN_CONNS", defaultMinConns),
		MaxConnIdleTime: getEnvDuration("PG_MAX_CONN_IDLE_TIME", defaultMaxConnIdleTime),
		MaxConnLifetime: getEnvDuration("PG_MAX_CONN_LIFETIME", defaultMaxConnLifetime),
	}
	return cfg, nil
}

func getEnvInt(key string, defaultValue int32) int32 {
	fromEnv := os.Getenv(key)
	if fromEnv == "" {
		slog.Warn("Value is set to default", "valueName", key)
		return defaultValue
	}
	parsedValue, err := strconv.ParseInt(fromEnv, 10, 32)
	if err != nil {
		slog.Warn("Value is set to default", "valueName", key, "rawData", fromEnv)
		return defaultValue
	}
	return int32(parsedValue)
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	fromEnv := os.Getenv(key)
	if fromEnv == "" {
		slog.Warn("Value is set to default", "valueName", key)
		return defaultValue
	}
	parsedValue, err := time.ParseDuration(fromEnv)
	if err != nil {
		slog.Warn("Value is set to default", "valueName", key, "rawData", fromEnv)
		return defaultValue
	}
	return parsedValue
}
