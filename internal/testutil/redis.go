// Package testutil содержит помощники интеграционных тестов.
package testutil

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
)

// StartRedis поднимает Redis в контейнере. cleanup закрывает клиент и останавливает контейнер.
func StartRedis(ctx context.Context) (client *redis.Client, cleanup func(), err error) {
	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		return nil, nil, fmt.Errorf("run redis container: %w", err)
	}
	terminate := func() { _ = testcontainers.TerminateContainer(container) } //nolint:gosec // очистка после тестов

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		terminate()
		return nil, nil, fmt.Errorf("redis connection string: %w", err)
	}
	c, err := rdb.New(ctx, &rdb.Config{URL: uri})
	if err != nil {
		terminate()
		return nil, nil, err
	}
	return c.Redis(), func() { _ = c.Close(); terminate() }, nil //nolint:gosec // очистка после тестов
}
