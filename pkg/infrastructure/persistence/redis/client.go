package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/anashasan/goqueue/pkg/common/errors"
	"github.com/anashasan/goqueue/pkg/infrastructure/persistence/redis/script"
)

// ClientConfig is the connection configuration, kept here rather than imported
// from the config package so that this package depends on nothing above it.
type ClientConfig struct {
	Addrs        []string
	Username     string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// NewClient builds a Redis client and verifies it can reach the server.
//
// A UniversalClient is used so the same code path serves a single node in
// development and a cluster in production: the deployment decides by how many
// addresses it supplies, and no application code changes.
func NewClient(ctx context.Context, cfg ClientConfig) (goredis.UniversalClient, error) {
	if len(cfg.Addrs) == 0 {
		cfg.Addrs = []string{"127.0.0.1:6379"}
	}

	client := goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs:        cfg.Addrs,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, errors.Wrap(
			errors.KindUnavailable,
			"redis_unavailable",
			"failed to connect to redis",
			err,
		)
	}

	return client, nil
}

// PreloadScripts pushes every Lua script into the server's script cache so the
// first job of the process pays the same cost as the millionth.
//
// A failure here is not fatal: go-redis falls back to EVAL and reloads the
// script transparently, so the caller logs and continues.
func PreloadScripts(ctx context.Context, client goredis.UniversalClient) error {
	for _, s := range script.All() {
		if err := s.Load(ctx, client).Err(); err != nil {
			return errors.Internal("script_preload_failed", "failed to preload lua script", err)
		}
	}
	return nil
}
