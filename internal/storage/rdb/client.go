// Package rdb содержит работу с Redis: подключение и ключи. Redis — кэш, лидерборд и rate limit,
// источник правды — Postgres.
package rdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config: URL в формате redis://[user:pass@]host:port/db.
type Config struct {
	URL          string
	PoolSize     int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Client — один на процесс: go-redis сам держит пул соединений и безопасен для горутин.
type Client struct {
	rc *redis.Client
}

// New разбирает URL, настраивает таймауты и проверяет доступность Redis.
func New(ctx context.Context, cfg *Config) (*Client, error) {
	if cfg.URL == "" {
		return nil, errors.New("redis url is empty")
	}
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	if cfg.PoolSize > 0 {
		opts.PoolSize = cfg.PoolSize
	}
	if cfg.DialTimeout > 0 {
		opts.DialTimeout = cfg.DialTimeout
	}
	if cfg.ReadTimeout > 0 {
		opts.ReadTimeout = cfg.ReadTimeout
	}
	if cfg.WriteTimeout > 0 {
		opts.WriteTimeout = cfg.WriteTimeout
	}

	c := &Client{rc: redis.NewClient(opts)}
	if err := c.Ping(ctx); err != nil {
		_ = c.rc.Close() //nolint:gosec // уже возвращаем ошибку Ping
		return nil, err
	}
	return c, nil
}

// Redis отдаёт go-redis клиент для репозиториев.
func (c *Client) Redis() *redis.Client { return c.rc }

func (c *Client) Ping(ctx context.Context) error {
	if err := c.rc.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}
	return nil
}

func (c *Client) Close() error { return c.rc.Close() }
